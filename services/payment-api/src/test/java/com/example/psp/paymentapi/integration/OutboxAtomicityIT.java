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

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("integration-test")
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "psp.provider-status-query.v1",
            "psp.provider-status-reply.v1",
            "merchants.merchant-config-changed.v1"
        })
class OutboxAtomicityIT {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
                    .withDatabaseName("payment_api")
                    .withUsername("payment_api")
                    .withPassword("payment_api");

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

    @BeforeEach
    void seedActiveMerchants() {
        Timestamp now = Timestamp.from(Instant.now());
        for (String merchantId : new String[] {MERCHANT_ID, "merchant-outbox-it-rollback"}) {
            jdbcTemplate.update(
                    "INSERT INTO merchant_configs "
                            + "(merchant_id, display_name, status, payout_currency, allowed_currencies, "
                            + "decline_rate_alert_threshold_bps, updated_at) "
                            + "VALUES (?, ?, 'ACTIVE', 'EUR', 'EUR', 1500, ?) "
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

        PaymentRequested staged = deserialize((byte[]) outbox.get("payload"));

        assertThat(staged.getPaymentId()).isEqualTo(paymentId.toString());
        assertThat(staged.getMerchantId()).isEqualTo(MERCHANT_ID);
        assertThat(staged.getAmount()).isEqualByComparingTo("19.99");
        assertThat(staged.getCurrency()).isEqualTo("EUR");
        assertThat(staged.getStatus()).isEqualTo(String.valueOf(payment.get("status")));

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
            return (PaymentRequested)
                    deserializer.deserialize("payments.payment-requested.v1", wireBytes);
        }
    }
}
