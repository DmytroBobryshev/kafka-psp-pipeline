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

    private static final int PAYMENT_COUNT = 8;

    private static final int DUPLICATE_COUNT = 3;

    private static final String MERCHANT_ID = "merchant-eos-it";

    private static final BigDecimal AMOUNT = new BigDecimal("25.0000");

    @Autowired private JdbcTemplate jdbcTemplate;

    @Autowired private KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @BeforeAll
    static void createIntegrationTopics() {
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
