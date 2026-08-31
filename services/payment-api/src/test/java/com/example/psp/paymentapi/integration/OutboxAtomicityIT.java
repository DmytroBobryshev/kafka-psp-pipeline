package com.example.psp.paymentapi.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.paymentapi.application.CreatePaymentCommand;
import com.example.psp.paymentapi.application.CreatePaymentUseCase;
import com.example.psp.paymentapi.domain.model.Money;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * M19 "Plus" for payment-api: <b>{@code POST /api/payments} writes the payment row and its outbox
 * row in ONE database transaction, and the staged payload really is a valid
 * {@code payments.payment-requested.v1} envelope.</b>
 *
 * <h2>Why there is no Kafka container here</h2>
 *
 * <p>payment-api (M6) does not publish to Kafka at all. {@code CreatePaymentUseCase} calls
 * {@code OutboxPaymentEventPublisher}, which Avro-serializes the event and INSERTs the bytes into
 * {@code outbox_event} - a plain row in the same database, in the same transaction as the payment.
 * Getting that row onto {@code payments.payment-requested.v1} is Debezium's job: it tails the
 * Postgres WAL from outside this process (infra/compose's {@code kafka-connect} +
 * {@code payment-outbox-connector.json}). A broker started here would sit idle: no code path in
 * this test could ever produce to it, so its presence would prove nothing and cost an image, a
 * container start and a chunk of the suite's wall-clock budget. The relay itself is exercised for
 * real against the compose stack and captured in the README, not faked here.
 *
 * <p>An {@code @EmbeddedKafka} broker IS present, for one unrelated reason: M12's
 * {@code ReplyingKafkaConfig} builds a {@code ReplyingKafkaTemplate} with its own reply container
 * at startup. That is a context-wiring concern, not part of what this IT asserts - the same
 * arrangement {@code PaymentApiApplicationTests} already uses. Postgres is the only Testcontainer.
 *
 * <h2>The two tests, and what "one transaction" is actually proved by</h2>
 *
 * <ol>
 *   <li><b>{@code postPaymentStagesBothRowsAndAValidEnvelope}</b> - the happy path: both rows
 *       exist, {@code outbox_event.aggregate_id} is the payment's id, and the {@code BYTEA} payload
 *       deserializes through a real {@code KafkaAvroDeserializer} into a {@link PaymentRequested}
 *       whose envelope and business fields match the row. That last part matters more than it
 *       looks: the outbox stores Confluent WIRE FORMAT (magic byte + schema id + Avro binary), so a
 *       payload that is merely non-null proves nothing - Debezium hands these exact bytes to the
 *       topic, and a consumer would be the first to find out if they were wrong.
 *   <li><b>{@code rollingBackTheCallerTransactionLeavesNeitherRow}</b> - the atomicity proof. The
 *       use case is invoked inside an outer, programmatically rolled-back transaction. Because
 *       {@code CreatePaymentUseCase} is {@code @Transactional} with the default
 *       {@code Propagation.REQUIRED}, both writes join that outer transaction and both must vanish.
 *       If the outbox insert ever moved onto its own transaction, its own connection, or
 *       {@code REQUIRES_NEW} - the realistic ways this gets broken - the outbox row would survive
 *       the rollback and this test would catch it. It needs no failure-injection hook, so there is
 *       no test-only seam in production code to keep honest.
 * </ol>
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("integration-test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "psp.provider-status-query.v1",
            "psp.provider-status-reply.v1",
            // config.MerchantViewKafkaConfig's merchant-view listener container subscribes at
            // context-refresh time, same requirement as the two topics above.
            "merchants.merchant-config-changed.v1"
        })
class OutboxAtomicityIT {

    /**
     * Real Postgres because the outbox is a database feature end to end: V2 creates the table, V3
     * turns {@code payload} into {@code BYTEA} (H2 would happily accept the wrong type), and V5 adds
     * {@code trace_parent}. Static singleton, started once per JVM; Ryuk reaps it at exit.
     */
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
                    .withDatabaseName("payment_api")
                    .withUsername("payment_api")
                    .withPassword("payment_api");

    /**
     * {@code mock://<scope>} makes Confluent's {@code SchemaRegistryClientFactory} return a
     * JVM-static {@code MockSchemaRegistryClient} instead of an HTTP client. The application's
     * {@code paymentRequestedAvroSerializer} bean and this test's deserializer share that one
     * client, so the schema id embedded in the stored bytes resolves - no registry container.
     */
    static final String SCHEMA_REGISTRY_URL = "mock://payment-api-it";

    private static final String MERCHANT_ID = "merchant-outbox-it";

    static {
        POSTGRES.start();
    }

    @Autowired private MockMvc mockMvc;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private CreatePaymentUseCase createPaymentUseCase;
    @Autowired private PlatformTransactionManager transactionManager;

    @DynamicPropertySource
    static void outboxItProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("payment-api.schema-registry.url", () -> SCHEMA_REGISTRY_URL);
    }

    /**
     * {@code POST /api/payments} now requires an {@code ACTIVE} merchant in the local projection
     * (see {@code CreatePaymentUseCase}); this IT never runs the Kafka listener that would
     * populate it, so it seeds the row directly - a Postgres-only shortcut this test can take
     * because it asserts on the outbox write, not on the projection itself.
     */
    @BeforeEach
    void seedActiveMerchants() {
        // Timestamp.from(...), not the raw Instant: the driver cannot infer a SQL type for
        // java.time.Instant through JdbcTemplate's varargs update() (no column type info to
        // consult, unlike a PreparedStatementSetter).
        Timestamp now = Timestamp.from(Instant.now());
        for (String merchantId : new String[] {MERCHANT_ID, "merchant-outbox-it-rollback"}) {
            jdbcTemplate.update(
                    "INSERT INTO merchant_configs "
                            + "(merchant_id, display_name, status, payout_currency, decline_rate_alert_threshold_bps, updated_at) "
                            + "VALUES (?, ?, 'ACTIVE', 'EUR', 1500, ?) "
                            + "ON CONFLICT (merchant_id) DO NOTHING",
                    merchantId,
                    merchantId,
                    now);
        }
    }

    @Test
    void postPaymentStagesBothRowsAndAValidEnvelope() throws Exception {
        MvcResult result =
                mockMvc.perform(
                                post("/api/payments")
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content(
                                                """
                                                {"merchantId":"%s","amount":19.99,"currency":"EUR"}
                                                """
                                                        .formatted(MERCHANT_ID)))
                        .andExpect(status().isCreated())
                        .andReturn();

        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        UUID paymentId = UUID.fromString(body.get("id").asText());

        Map<String, Object> payment =
                jdbcTemplate.queryForMap("SELECT * FROM payments WHERE id = ?", paymentId);
        assertThat(payment.get("merchant_id")).isEqualTo(MERCHANT_ID);
        assertThat((BigDecimal) payment.get("amount")).isEqualByComparingTo("19.99");

        Map<String, Object> outbox =
                jdbcTemplate.queryForMap(
                        "SELECT * FROM outbox_event WHERE aggregate_id = ?", paymentId.toString());
        assertThat(outbox.get("aggregate_type")).isEqualTo("payment");
        assertThat(outbox.get("event_type")).isEqualTo("payments.payment-requested.v1");

        // The row is only useful if Debezium can hand these exact bytes to a consumer, so decode
        // them the way that consumer will: Confluent wire format, specific reader, same registry.
        PaymentRequested staged = deserialize((byte[]) outbox.get("payload"));

        assertThat(staged.getPaymentId()).isEqualTo(paymentId.toString());
        assertThat(staged.getMerchantId()).isEqualTo(MERCHANT_ID);
        assertThat(staged.getAmount()).isEqualByComparingTo("19.99");
        assertThat(staged.getCurrency()).isEqualTo("EUR");
        assertThat(staged.getStatus()).isEqualTo(String.valueOf(payment.get("status")));

        // ADR-0002: the envelope's own eventId IS the outbox row's primary key. That is what makes
        // the id stable across a Debezium re-snapshot or a topic replay, and it is the idempotency
        // key psp-connector (M5 level 1) and the ledger both dedup on.
        assertThat(staged.getEnvelope().getEventId()).isEqualTo(String.valueOf(outbox.get("id")));
        assertThat(staged.getEnvelope().getEventType()).isEqualTo("payments.payment-requested.v1");
        assertThat(staged.getEnvelope().getAggregateId()).isEqualTo(paymentId.toString());
        assertThat(staged.getEnvelope().getSource()).isEqualTo("payment-api");
        assertThat(staged.getEnvelope().getTraceId()).isNotBlank();
        assertThat(staged.getEnvelope().getCorrelationId()).isNotBlank();
    }

    @Test
    void rollingBackTheCallerTransactionLeavesNeitherRow() {
        String merchantId = "merchant-outbox-it-rollback";
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        UUID paymentId =
                transactionTemplate.execute(
                        transactionStatus -> {
                            UUID id =
                                    createPaymentUseCase
                                            .execute(
                                                    new CreatePaymentCommand(
                                                            merchantId, Money.of(new BigDecimal("7.77"), "EUR")))
                                            .getId();
                            // Both writes have happened; nothing is committed yet. Rolling back here
                            // is the whole assertion: if the outbox insert did not join THIS
                            // transaction, it would survive.
                            transactionStatus.setRollbackOnly();
                            return id;
                        });

        assertThat(
                        jdbcTemplate.queryForObject(
                                "SELECT count(*) FROM payments WHERE id = ?", Integer.class, paymentId))
                .as("the payment row must be gone after the rollback")
                .isZero();

        assertThat(
                        jdbcTemplate.queryForObject(
                                "SELECT count(*) FROM outbox_event WHERE aggregate_id = ?",
                                Integer.class,
                                paymentId.toString()))
                .as(
                        "and so must its outbox row - a surviving row here means the two writes are NOT "
                                + "one transaction, which is the entire point of the outbox pattern")
                .isZero();
    }

    private static PaymentRequested deserialize(byte[] wireBytes) {
        try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer()) {
            deserializer.configure(
                    Map.of(
                            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL,
                            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true),
                    false);
            // The "topic" argument is the same string OutboxPaymentEventPublisher passes to
            // serialize() - the event type, which is also the subject prefix under TopicNameStrategy
            // (ADR-0001). The schema itself is resolved from the id embedded in the bytes.
            return (PaymentRequested)
                    deserializer.deserialize("payments.payment-requested.v1", wireBytes);
        }
    }
}
