package com.example.psp.pspconnector.integration;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.PaymentRequested;
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
import java.util.Set;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared plumbing for psp-connector's M19 "Plus" integration tests ({@code RebalanceLossIT},
 * {@code CrashRedeliveryIT}). Not a test itself - the name deliberately avoids surefire's
 * {@code *Test.java} include patterns as well as failsafe's {@code *IT.java} ones.
 *
 * <h2>Why the containers are static singletons</h2>
 *
 * <p>{@link #KAFKA} and {@link #POSTGRES} are {@code static final} and started once from a static
 * initialiser, NOT annotated with {@code @Container}. That is Testcontainers' documented singleton
 * pattern, and the choice matters here: psp-connector has two IT classes with two different Spring
 * contexts (see {@code CrashRedeliveryIT}'s {@code @TestConfiguration}), and the per-class
 * {@code @Container} lifecycle would boot and tear down a fresh broker and a fresh Postgres for
 * each of them. One broker start (~4 s) is most of this module's IT wall-clock; paying it twice
 * doubles the suite for no extra coverage. Ryuk (the {@code testcontainers/ryuk} sidecar) reaps
 * both containers when the surefire/failsafe JVM exits, so nothing leaks.
 *
 * <h2>Why the two IT classes still do not interfere</h2>
 *
 * <p>Sharing one broker and one database means sharing state, so isolation is bought a different
 * way: every IT overrides {@code psp-connector.kafka.*-topic} and
 * {@code spring.kafka.consumer.group-id} to values of its own (see each subclass's
 * {@code @DynamicPropertySource}), and asserts against its own {@code merchantId} in Postgres.
 * Topic names are configuration in this codebase, never constants, which is exactly what makes
 * that possible.
 *
 * <h2>Why there is no Schema Registry container</h2>
 *
 * <p>Every Avro topic here is Confluent-wire-format, so the serializers need a registry - but not
 * necessarily an HTTP one. Confluent's {@code SchemaRegistryClientFactory} special-cases a
 * {@code mock://<scope>} URL and hands back a {@link
 * io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient} from a JVM-static, per-scope
 * map ({@code io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry}). Because an
 * {@code @SpringBootTest} runs the application inside the SAME JVM as the test, the application's
 * {@code KafkaAvroSerializer}/{@code KafkaAvroDeserializer} and the test's own serializers below
 * all resolve to that one shared client as long as they use the same scope
 * ({@link #SCHEMA_REGISTRY_URL}) - identical schema ids, identical wire bytes, no HTTP, no third
 * container, no {@code cp-schema-registry} image to pull. The registry is not what these tests are
 * proving; the broker and the database are.
 *
 * <h2>Why {@code @ActiveProfiles("integration-test")}</h2>
 *
 * <p>{@code src/test/resources/application.yml} shadows (does not merge with) the production
 * {@code application.yml} and omits the entire {@code spring.kafka.producer} block. The
 * {@code integration-test} profile file adds it back - see that file's header comment for the full
 * story and the exact failure it prevents.
 */
@ActiveProfiles("integration-test")
abstract class PspConnectorIntegrationSupport {

    /**
     * KRaft-mode Apache Kafka. {@code org.testcontainers.kafka.KafkaContainer} (1.20.1+) wraps the
     * vendor-neutral {@code apache/kafka} image and already sets the single-broker essentials -
     * {@code offsets.topic.replication.factor=1}, {@code transaction.state.log.replication.factor=1},
     * {@code transaction.state.log.min.isr=1}, {@code group.initial.rebalance.delay.ms=0} - so
     * nothing here has to. The last of those matters for {@code RebalanceLossIT}: without it every
     * rejoin would sit in the coordinator's 3 s batching window.
     */
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    /**
     * Real Postgres, because Flyway V1-V4 are part of what is under test: {@code CrashRedeliveryIT}
     * asserts on {@code payment_attempts.status_event_id}, a column V4 adds, and the M5 dedup that
     * both ITs depend on is enforced by V1/V2 unique constraints. H2 in PostgreSQL mode (what the
     * unit-test {@code src/test/resources/application.yml} uses) would run none of those migrations.
     */
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
                    .withDatabaseName("psp_connector")
                    .withUsername("psp_connector")
                    .withPassword("psp_connector");

    /** See the class javadoc - a JVM-local mock registry, shared by the app and this test. */
    static final String SCHEMA_REGISTRY_URL = "mock://psp-connector-it";

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    @Autowired protected KafkaListenerEndpointRegistry listenerEndpointRegistry;

    /**
     * The overrides every psp-connector IT needs. {@code @DynamicPropertySource} wins over
     * {@code src/test/resources/application.yml}, which is how the H2 + {@code flyway.enabled=false}
     * unit-test profile is turned back into "real Postgres, migrations on" without touching that
     * file (the existing {@code PspConnectorApplicationTests} still uses it as-is).
     *
     * <p>{@code ddl-auto=none} rather than {@code validate}: Flyway owns the schema here, and
     * Hibernate validation of a hand-written migration adds a second, unrelated way for these tests
     * to go red.
     */
    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);

        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> true);

        registry.add("psp-connector.schema-registry.url", () -> SCHEMA_REGISTRY_URL);

        // Determinism: the simulated provider's 10% decline / 5% timeout dice would make any
        // exact accounting assertion below a coin flip. APPROVED forces ADR-0006 category B on
        // every call, which is the only outcome that always publishes a status event.
        registry.add("psp-connector.provider.forced-outcome", () -> "APPROVED");
        registry.add("psp-connector.provider.duplicate-rate", () -> "0.0");
    }

    // ---------------------------------------------------------------------------------------
    // Topic administration
    // ---------------------------------------------------------------------------------------

    /**
     * Creates topics up front instead of relying on the broker's {@code auto.create.topics.enable}.
     * Auto-creation would work, but it creates a topic with the broker's default partition count at
     * the moment a consumer first asks for metadata - a race that shows up as an empty assignment
     * and a flaky test, not as a clear failure.
     */
    protected static void createTopics(Map<String, Integer> topicToPartitions) {
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

    // ---------------------------------------------------------------------------------------
    // Producing the inbound event
    // ---------------------------------------------------------------------------------------

    /** A plain Avro producer standing in for payment-api's outbox + Debezium relay. */
    protected static Producer<String, Object> avroProducer() {
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
     * Builds a {@code payments.payment-requested.v1} record byte-identical in shape to what
     * payment-api stages in its outbox. {@code eventId} is passed in rather than minted here on
     * purpose: replaying the SAME {@code eventId} is how these tests exercise M5 level 1 / M19
     * drill 9's republish path.
     */
    protected static PaymentRequested paymentRequested(
            UUID eventId, UUID paymentId, String merchantId, BigDecimal amount) {
        EventEnvelope envelope =
                EventEnvelope.newBuilder()
                        .setEventId(eventId.toString())
                        .setEventType("payments.payment-requested.v1")
                        .setEventVersion(1)
                        .setAggregateId(paymentId.toString())
                        .setAggregateType("payment")
                        .setOccurredAt(Instant.now())
                        .setSource("psp-connector-it")
                        .setTraceId(eventId.toString().replace("-", "").substring(0, 32))
                        .setCorrelationId(eventId.toString())
                        .setCausationId(null)
                        .build();
        return PaymentRequested.newBuilder()
                .setEnvelope(envelope)
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId)
                // Scale MUST be exactly 4 - 02-payment-requested.avsc declares
                // decimal(precision=19, scale=4), and Avro's decimal Conversion rejects anything
                // whose scale does not match the writer schema.
                .setAmount(amount.setScale(4))
                .setCurrency("EUR")
                .setStatus("PENDING")
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // Consuming the outbound event
    // ---------------------------------------------------------------------------------------

    /**
     * A verification consumer for {@code payments.payment-status-changed.v1}. {@code read_committed}
     * mirrors what every real consumer group in this system sets (docs/diagrams/topic-map.md); a
     * fresh random {@code group.id} plus {@code earliest} guarantees it reads the whole topic from
     * offset 0 no matter what the application's own group has committed.
     */
    protected static KafkaConsumer<String, PaymentStatusChanged> statusChangedConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "psp-connector-it-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(props);
    }

    /**
     * Drains {@code topic} until {@code done} accepts the accumulated list or {@code timeout}
     * elapses, then returns everything seen. Deliberately returns rather than asserts: "how many
     * records arrived, and were any of them duplicates" is the actual subject of these tests, so the
     * caller needs the full list even on the timeout path to produce a useful failure message.
     */
    protected static <V> List<ConsumerRecord<String, V>> drainUntil(
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

    // ---------------------------------------------------------------------------------------
    // Listener container access (the rebalance lever)
    // ---------------------------------------------------------------------------------------

    /**
     * Finds the {@code @KafkaListener} container subscribed to {@code topic}.
     *
     * <p>Looked up by topic rather than by listener id because the production
     * {@code @KafkaListener} annotations carry no {@code id} - and adding one purely so a test can
     * address them would be the test dictating production code. psp-connector runs three listener
     * containers (payment-requested, funds-reserved, provider-status-query) and their topic sets are
     * disjoint, so the topic is an unambiguous key.
     */
    protected MessageListenerContainer listenerContainerFor(String topic) {
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

    /** Convenience for the "record on the wire" step of the tests. */
    protected static void send(Producer<String, Object> producer, String topic, String key, Object value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    // ---------------------------------------------------------------------------------------
    // M21: terminal-vs-trail filtering
    // ---------------------------------------------------------------------------------------

    /**
     * PENDING/IPN_RECEIVED/VERIFIED (M20/M21) are non-terminal trail events every payment now emits
     * BEFORE its one terminal SUCCEEDED/DECLINED event. The "no loss / no double-count" accounting
     * in {@code RebalanceLossIT} and {@code CrashRedeliveryIT} is about the terminal event
     * specifically (republish() only ever re-emits that one, under the stored statusEventId - see
     * {@code ProcessPaymentRequestUseCase}) - counting/grouping the raw, unfiltered record stream
     * would count each payment's trail events too and break those assertions.
     */
    protected static final Set<String> TERMINAL_STATUSES = Set.of("SUCCEEDED", "DECLINED");

    protected static List<ConsumerRecord<String, PaymentStatusChanged>> terminalOnly(
            List<ConsumerRecord<String, PaymentStatusChanged>> records) {
        return records.stream().filter(r -> TERMINAL_STATUSES.contains(r.value().getStatus())).toList();
    }
}
