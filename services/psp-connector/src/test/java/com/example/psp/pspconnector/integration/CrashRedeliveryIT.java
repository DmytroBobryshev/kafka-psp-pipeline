package com.example.psp.pspconnector.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.pspconnector.adapters.out.kafka.KafkaPaymentStatusPublisher;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CrashRedeliveryIT extends PspConnectorIntegrationSupport {

    private static final Logger log = LoggerFactory.getLogger(CrashRedeliveryIT.class);

    private static final String REQUESTED_TOPIC = "payments.payment-requested.v1.crash-it";

    private static final String STATUS_TOPIC = "payments.payment-status-changed.v1.crash-it";

    private static final String MERCHANT_ID = "merchant-crash-it";

    private static final AtomicBoolean PUBLISH_FAILING = new AtomicBoolean(true);

    @Autowired private JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void createIntegrationTopics() {
        createTopics(Map.of(REQUESTED_TOPIC, 1, STATUS_TOPIC, 1));
    }

    @DynamicPropertySource
    static void crashItProperties(DynamicPropertyRegistry registry) {
        registry.add("psp-connector.kafka.payment-requested-topic", () -> REQUESTED_TOPIC);
        registry.add("psp-connector.kafka.payment-status-changed-topic", () -> STATUS_TOPIC);
        registry.add("spring.kafka.consumer.group-id", () -> "psp-connector.crash-it.v1");
    }

    @Test
    void aRowWithoutItsEventIsRepairedOnRedeliveryUnderTheStoredEventId() throws Exception {
        MessageListenerContainer listener = listenerContainerFor(REQUESTED_TOPIC);
        ContainerTestUtils.waitForAssignment(listener, 1);

        UUID eventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        try (Producer<String, Object> producer = avroProducer();
                KafkaConsumer<String, PaymentStatusChanged> verifier = statusChangedConsumer()) {

            send(
                    producer,
                    REQUESTED_TOPIC,
                    paymentId.toString(),
                    paymentRequested(eventId, paymentId, MERCHANT_ID, new BigDecimal("42.50")));
            producer.flush();

            // ---- step 2: the loss window is open --------------------------------------------
            awaitAttemptRow(paymentId);

            List<ConsumerRecord<String, PaymentStatusChanged>> beforeRecovery =
                    drainUntil(verifier, STATUS_TOPIC, Duration.ofSeconds(5), records -> false);
            assertThat(terminalOnly(beforeRecovery))
                    .as(
                            "the attempt row is committed but the terminal publish keeps failing - this is "
                                    + "exactly the drill-9 window, and no TERMINAL status event may be on the "
                                    + "topic yet")
                    .isEmpty();

            // ---- step 3: "the pod came back" ------------------------------------------------
            PUBLISH_FAILING.set(false);
            log.info("--- publisher healed; bouncing the listener container to force redelivery ---");
            listener.stop();
            listener.start();
            ContainerTestUtils.waitForAssignment(listener, 1);

            List<ConsumerRecord<String, PaymentStatusChanged>> recovered =
                    drainUntil(
                            verifier, STATUS_TOPIC, Duration.ofSeconds(60), records -> !records.isEmpty());
            List<ConsumerRecord<String, PaymentStatusChanged>> settle =
                    drainUntil(verifier, STATUS_TOPIC, Duration.ofSeconds(5), records -> false);
            List<ConsumerRecord<String, PaymentStatusChanged>> statusEvents =
                    Stream.concat(recovered.stream(), settle.stream()).toList();

            assertThat(statusEvents)
                    .as(
                            "redelivery MUST republish the stored attempt - pre-drill-9 the dedup check "
                                    + "skipped instead and this stayed 0 forever")
                    .hasSize(1);

            Map<String, Object> row =
                    jdbcTemplate.queryForMap(
                            "SELECT status_event_id, outcome FROM payment_attempts WHERE merchant_id = ?",
                            MERCHANT_ID);

            assertThat(row.get("outcome"))
                    .as("forced-outcome=APPROVED, so the row is a category-B business outcome")
                    .isEqualTo("APPROVED");

            assertThat(statusEvents.get(0).value().getEnvelope().getEventId())
                    .as(
                            "the republished envelope eventId must be the row's stored status_event_id, not "
                                    + "a fresh mint - downstream dedup only recognises a byte-identical key")
                    .isEqualTo(String.valueOf(row.get("status_event_id")));

            assertThat(statusEvents.get(0).value().getPaymentId()).isEqualTo(paymentId.toString());

            List<String> rowsOwedAnEvent =
                    jdbcTemplate.queryForList(
                            "SELECT status_event_id FROM payment_attempts "
                                    + "WHERE merchant_id = ? AND outcome <> 'TIMEOUT'",
                            String.class,
                            MERCHANT_ID);
            assertThat(
                            statusEvents.stream()
                                    .map(record -> record.value().getEnvelope().getEventId())
                                    .toList())
                    .as(
                            "every non-TIMEOUT payment_attempts row must have its status event on the "
                                    + "topic, matched by status_event_id")
                    .containsExactlyInAnyOrderElementsOf(rowsOwedAnEvent);
        }
    }

    private void awaitAttemptRow(UUID paymentId) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(60);
        while (Instant.now().isBefore(deadline)) {
            Integer rows =
                    jdbcTemplate.queryForObject(
                            "SELECT count(*) FROM payment_attempts WHERE payment_id = ?",
                            Integer.class,
                            paymentId);
            if (rows != null && rows > 0) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "the payment_attempts row for paymentId=" + paymentId + " was never written");
    }

    @TestConfiguration
    static class FailFirstPublishConfiguration {

        @Bean
        @Primary
        PaymentStatusPublisher failingPaymentStatusPublisher(KafkaPaymentStatusPublisher delegate) {
            return new PaymentStatusPublisher() {
                @Override
                public void publishPending(
                        java.util.UUID paymentId,
                        String merchantId,
                        com.example.psp.pspconnector.domain.model.Money amount,
                        java.util.UUID causationEventId,
                        String traceId,
                        String correlationId) {
                    delegate.publishPending(
                            paymentId, merchantId, amount, causationEventId, traceId, correlationId);
                }

                @Override
                public void publishIpnReceived(
                        java.util.UUID paymentId,
                        String merchantId,
                        com.example.psp.pspconnector.domain.model.Money amount,
                        java.util.UUID providerReference,
                        java.util.UUID causationEventId,
                        String traceId,
                        String correlationId) {
                    delegate.publishIpnReceived(
                            paymentId, merchantId, amount, providerReference, causationEventId, traceId,
                            correlationId);
                }

                @Override
                public void publishVerified(
                        java.util.UUID paymentId,
                        String merchantId,
                        com.example.psp.pspconnector.domain.model.Money amount,
                        java.util.UUID providerReference,
                        java.util.UUID causationEventId,
                        String traceId,
                        String correlationId) {
                    delegate.publishVerified(
                            paymentId, merchantId, amount, providerReference, causationEventId, traceId,
                            correlationId);
                }

                @Override
                public void publishStatusChanged(PaymentAttempt attempt) {
                    if (PUBLISH_FAILING.get()) {
                        throw new KafkaException(
                                "simulated publish failure AFTER the payment_attempts row was committed, "
                                        + "paymentId=" + attempt.getPaymentId());
                    }
                    delegate.publishStatusChanged(attempt);
                }
            };
        }
    }
}
