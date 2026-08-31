package com.example.psp.ledger.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.LedgerEntryRecorded;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * M19 "Plus" for the ledger: <b>{@code M} distinct payment-status events, delivered {@code M + D}
 * times, produce exactly {@code M} ledger entries, exactly {@code M} published
 * {@code ledger.ledger-entry-recorded.v1} records visible under {@code read_committed}, and a
 * balance equal to the sum of the {@code M}.</b>
 *
 * <h2>The two mechanisms this exercises, and which one actually does the work</h2>
 *
 * <p>M7's README makes a claim worth having a test for: Kafka's exactly-once semantics are NOT what
 * keeps the balance correct. Both mechanisms are live here and each is asserted from the side it
 * governs:
 *
 * <ul>
 *   <li><b>The Kafka transaction</b> ({@code config.KafkaProducerConfig}'s
 *       {@code transactional.id} + the container's {@code KafkaAwareTransactionManager}) covers
 *       Kafka only: the produced record and the consumed offset commit together, atomically. Its
 *       visible consequence is the {@code read_committed} count on the outbound topic - which is
 *       what a real broker is needed for, since a transaction coordinator and an LSO are not
 *       things a mock can fake.
 *   <li><b>{@code uq_ledger_entries_inbound_event_id}</b> (V1) covers the database, which no Kafka
 *       transaction can reach. Its visible consequence is that the {@value #DUPLICATE_COUNT}
 *       redelivered events - byte-identical envelope {@code eventId}s, exactly what psp-connector's
 *       drill-9 republish produces - move no money. Drop the constraint and the Kafka transaction
 *       keeps working while the balance quietly doubles for those payments.
 * </ul>
 *
 * <p>Deliveries are addressed to a single merchant on purpose: {@code merchant_balances} is one row
 * per merchant, so this is also the single-writer-per-balance path, and {@code balance = M * amount}
 * only holds if nothing double-applied.
 *
 * <h2>Containers</h2>
 *
 * <p>Real KRaft Kafka ({@code apache/kafka} via {@code org.testcontainers.kafka.KafkaContainer},
 * which already sets {@code transaction.state.log.replication.factor=1} and
 * {@code transaction.state.log.min.isr=1} - a transactional producer refuses to start on a
 * single-broker cluster without them) and real Postgres, so Flyway V1-V2 create the actual
 * constraint under test. Both are {@code static} singletons started once per JVM rather than
 * {@code @Container}-managed per class; Ryuk reaps them at JVM exit.
 *
 * <p>No Schema Registry container: {@code mock://<scope>} makes Confluent's serializers resolve a
 * JVM-static {@code MockSchemaRegistryClient}, shared by the application (same JVM, it is an
 * {@code @SpringBootTest}) and by this test's own producer/consumer. Identical schema ids,
 * identical wire bytes, one fewer image.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration-test")
class LedgerEosIT {

    private static final Logger log = LoggerFactory.getLogger(LedgerEosIT.class);

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
                    .withDatabaseName("ledger")
                    .withUsername("ledger")
                    .withPassword("ledger");

    static final String SCHEMA_REGISTRY_URL = "mock://ledger-it";

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    private static final String STATUS_TOPIC = "payments.payment-status-changed.v1.eos-it";

    private static final String ENTRY_TOPIC = "ledger.ledger-entry-recorded.v1.eos-it";

    /** Distinct payments. */
    private static final int PAYMENT_COUNT = 8;

    /** How many of those are ALSO delivered a second time, under the same envelope eventId. */
    private static final int DUPLICATE_COUNT = 3;

    private static final String MERCHANT_ID = "merchant-eos-it";

    private static final BigDecimal AMOUNT = new BigDecimal("25.0000");

    @Autowired private JdbcTemplate jdbcTemplate;

    @Autowired private KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @BeforeAll
    static void createIntegrationTopics() {
        // The two IT-scoped topics, plus the refunds.* topics the ledger's OTHER listeners
        // (M11 saga) subscribe to - created up front so those containers get a clean assignment
        // instead of racing the broker's auto-creation.
        createTopics(
                Map.of(
                        STATUS_TOPIC, 1,
                        ENTRY_TOPIC, 1,
                        "refunds.refund-requested.v1", 1,
                        "refunds.refund-completed.v1", 1,
                        "refunds.refund-failed.v1", 1,
                        "refunds.funds-reserved.v1", 1,
                        "refunds.reservation-released.v1", 1));
    }

    @DynamicPropertySource
    static void ledgerItProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);

        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        // Flyway owns the schema here - V1's uq_ledger_entries_inbound_event_id is under test.
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> true);

        registry.add("ledger.schema-registry.url", () -> SCHEMA_REGISTRY_URL);
        registry.add("ledger.kafka.payment-status-changed-topic", () -> STATUS_TOPIC);
        registry.add("ledger.kafka.ledger-entry-recorded-topic", () -> ENTRY_TOPIC);
        registry.add("spring.kafka.consumer.group-id", () -> "ledger.eos-it.v1");
        // transactional.id must be stable per logical instance (KafkaProducerConfig's javadoc
        // spends three paragraphs on why); "one IT run = one instance" makes this one right.
        registry.add("ledger.kafka.transactional-id-prefix", () -> "ledger-tx-eos-it-");
    }

    @Test
    void duplicateDeliveriesProduceNoExtraEntriesEventsOrBalance() {
        MessageListenerContainer listener = listenerContainerFor(STATUS_TOPIC);
        ContainerTestUtils.waitForAssignment(listener, 1);

        List<UUID> eventIds = new ArrayList<>();
        try (Producer<String, Object> producer = avroProducer()) {
            for (int i = 0; i < PAYMENT_COUNT; i++) {
                UUID eventId = UUID.randomUUID();
                eventIds.add(eventId);
                producer.send(
                        new ProducerRecord<>(
                                STATUS_TOPIC, MERCHANT_ID, succeeded(eventId, UUID.randomUUID())));
            }
            producer.flush();

            // The redeliveries. Same envelope eventId, same payload - exactly the shape
            // psp-connector's M19 drill-9 republish puts on this topic, and exactly what an
            // operator resetting this group's offsets to earliest would replay.
            for (int i = 0; i < DUPLICATE_COUNT; i++) {
                UUID eventId = eventIds.get(i);
                producer.send(
                        new ProducerRecord<>(
                                STATUS_TOPIC, MERCHANT_ID, succeeded(eventId, UUID.randomUUID())));
            }
            producer.flush();
        }

        List<ConsumerRecord<String, LedgerEntryRecorded>> published;
        try (KafkaConsumer<String, LedgerEntryRecorded> verifier = entryRecordedConsumer()) {
            published =
                    drainUntil(
                            verifier,
                            ENTRY_TOPIC,
                            Duration.ofSeconds(90),
                            records -> records.size() >= PAYMENT_COUNT);
            // Give a spurious extra record a fair chance to appear before asserting "exactly M".
            List<ConsumerRecord<String, LedgerEntryRecorded>> settle =
                    drainUntil(verifier, ENTRY_TOPIC, Duration.ofSeconds(5), records -> false);
            published = new ArrayList<>(published);
            published.addAll(settle);
        }

        log.info(
                "LedgerEosIT: sent {} events ({} of them twice), saw {} ledger-entry-recorded records",
                PAYMENT_COUNT + DUPLICATE_COUNT,
                DUPLICATE_COUNT,
                published.size());

        assertThat(published)
                .as(
                        "exactly one ledger.ledger-entry-recorded.v1 per DISTINCT inbound event, read under "
                                + "isolation.level=read_committed - %d deliveries in, %d entries out",
                        PAYMENT_COUNT + DUPLICATE_COUNT, PAYMENT_COUNT)
                .hasSize(PAYMENT_COUNT);

        assertThat(published)
                .as("every published entry is a CREDIT for this merchant, at the amount sent")
                .allSatisfy(
                        record -> {
                            assertThat(record.value().getMerchantId()).isEqualTo(MERCHANT_ID);
                            assertThat(record.value().getDirection()).isEqualTo("CREDIT");
                            assertThat(record.value().getAmount()).isEqualByComparingTo(AMOUNT);
                        });

        Integer entryRows =
                jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM ledger_entries WHERE merchant_id = ?",
                        Integer.class,
                        MERCHANT_ID);
        assertThat(entryRows)
                .as(
                        "uq_ledger_entries_inbound_event_id - not the Kafka transaction - is what makes the "
                                + "%d redeliveries move no money",
                        DUPLICATE_COUNT)
                .isEqualTo(PAYMENT_COUNT);

        Map<String, Object> balance =
                jdbcTemplate.queryForMap(
                        "SELECT balance, entry_count FROM merchant_balances WHERE merchant_id = ?",
                        MERCHANT_ID);
        assertThat((BigDecimal) balance.get("balance"))
                .as("balance must be the sum of the %d applied entries, not of the %d deliveries",
                        PAYMENT_COUNT, PAYMENT_COUNT + DUPLICATE_COUNT)
                .isEqualByComparingTo(AMOUNT.multiply(BigDecimal.valueOf(PAYMENT_COUNT)));
        assertThat(((Number) balance.get("entry_count")).intValue())
                .as("entry_count is maintained by the same UPSERT as balance - it must agree with COUNT(*)")
                .isEqualTo(PAYMENT_COUNT);

        Integer distinctInboundIds =
                jdbcTemplate.queryForObject(
                        "SELECT count(DISTINCT inbound_event_id) FROM ledger_entries WHERE merchant_id = ?",
                        Integer.class,
                        MERCHANT_ID);
        assertThat(distinctInboundIds)
                .as("one row per inbound envelope eventId, by construction of the unique constraint")
                .isEqualTo(PAYMENT_COUNT);
    }

    // ---------------------------------------------------------------------------------------
    // Fixtures
    // ---------------------------------------------------------------------------------------

    /** A {@code payments.payment-status-changed.v1} exactly as psp-connector publishes it. */
    private static PaymentStatusChanged succeeded(UUID eventId, UUID paymentId) {
        EventEnvelope envelope =
                EventEnvelope.newBuilder()
                        .setEventId(eventId.toString())
                        .setEventType("payments.payment-status-changed.v1")
                        .setEventVersion(1)
                        .setAggregateId(paymentId.toString())
                        .setAggregateType("payment")
                        .setOccurredAt(Instant.now())
                        .setSource("ledger-eos-it")
                        .setTraceId(eventId.toString().replace("-", "").substring(0, 32))
                        .setCorrelationId(eventId.toString())
                        .setCausationId(null)
                        .build();
        return PaymentStatusChanged.newBuilder()
                .setEnvelope(envelope)
                .setPaymentId(paymentId.toString())
                .setMerchantId(MERCHANT_ID)
                // scale 4 - 03-payment-status-changed.avsc declares decimal(19,4) and Avro's
                // decimal conversion rejects any other scale.
                .setAmount(AMOUNT)
                .setCurrency("EUR")
                .setStatus("SUCCEEDED")
                .setProviderReference(UUID.randomUUID().toString())
                .setDeclineReason(null)
                .build();
    }

    private static Producer<String, Object> avroProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        return new KafkaProducer<>(props);
    }

    /**
     * {@code isolation.level=read_committed} is the assertion, not a detail: it is what makes the
     * count reflect only records whose Kafka transaction committed. Under the default
     * {@code read_uncommitted} an aborted transaction's records would be counted too, and the
     * "exactly M" assertion would be measuring something else entirely.
     */
    private static KafkaConsumer<String, LedgerEntryRecorded> entryRecordedConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger-eos-it-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(props);
    }

    private static <V> List<ConsumerRecord<String, V>> drainUntil(
            KafkaConsumer<String, V> consumer,
            String topic,
            Duration timeout,
            Predicate<List<ConsumerRecord<String, V>>> done) {
        consumer.subscribe(List.of(topic));
        List<ConsumerRecord<String, V>> collected = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, V> polled = consumer.poll(Duration.ofMillis(250));
            polled.forEach(collected::add);
            if (done.test(collected)) {
                return collected;
            }
        }
        return collected;
    }

    private static void createTopics(Map<String, Integer> topicToPartitions) {
        Map<String, Object> props =
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            List<NewTopic> topics = new ArrayList<>();
            topicToPartitions.forEach(
                    (topic, partitions) -> topics.add(new NewTopic(topic, partitions, (short) 1)));
            admin.createTopics(topics).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while creating topics", e);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("failed to create topics " + topicToPartitions, e);
            }
        }
    }

    /** By topic, not by listener id - the production {@code @KafkaListener}s declare no id. */
    private MessageListenerContainer listenerContainerFor(String topic) {
        return listenerEndpointRegistry.getListenerContainers().stream()
                .filter(
                        container -> {
                            String[] topics = container.getContainerProperties().getTopics();
                            return topics != null && Arrays.asList(topics).contains(topic);
                        })
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "no @KafkaListener container is subscribed to " + topic));
    }
}
