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

/**
 * M19 drill 9's regression test, against a real broker and a real Postgres: <b>an attempt row that
 * exists without its status event gets that event on redelivery, under the SAME eventId.</b>
 *
 * <h2>The defect this locks down</h2>
 *
 * <p>{@code ProcessPaymentRequestUseCase} writes the {@code payment_attempts} row and only then
 * publishes {@code payments.payment-status-changed.v1}. Those two writes go to two different
 * systems and cannot be one transaction, so there is a window in which the row exists and the event
 * does not. Drill 9 hit exactly that window - a KEDA scale-in killed a pod inside it - and the
 * original M5 code made the loss permanent: on redelivery the level-1 dedup check found the row and
 * <em>skipped</em>, so the event was never produced at all. Three payments vanished silently.
 *
 * <p>The fix (see {@code ProcessPaymentRequestUseCase#republish}) is that a dedup hit REPUBLISHES
 * from the stored row instead of skipping, reusing the row's {@code status_event_id} (V4 column) as
 * the envelope {@code eventId} so downstream idempotency still recognises it as one event.
 *
 * <h2>How the window is opened, and how it is closed</h2>
 *
 * <ol>
 *   <li><b>Open it.</b> {@link FailFirstPublishConfiguration} installs a {@code @Primary} decorator
 *       around the real {@link KafkaPaymentStatusPublisher} that throws while {@link
 *       #PUBLISH_FAILING} is set. The test drives that flag explicitly rather than counting
 *       failures, so the outcome does not depend on how the container's {@code DefaultErrorHandler}
 *       happens to classify the exception or on how many times it retries: every publish fails
 *       until the test says otherwise. Result: the attempt row is committed, nothing is on the
 *       topic, and the inbound offset is never committed (the listener never reaches
 *       {@code ack.acknowledge()}).
 *   <li><b>Verify the window is really open</b> - a row in Postgres, zero records on the topic.
 *       This is the state drill 9 left three real payments in.
 *   <li><b>Close it.</b> Clear the flag and stop/start the listener container. That is the closest
 *       a single JVM gets to "the pod came back": a new consumer joins the group, finds no
 *       committed offset for the partition, and redelivers the record. The redelivery hits M5 level
 *       1 and takes the republish path.
 * </ol>
 *
 * <p><b>Honest limitation:</b> the JVM, the Spring context, and the Postgres connection pool all
 * survive step 3 - only the Kafka consumer is recycled. The <em>state</em> under test (committed
 * row, uncommitted offset, absent event) and the code path taken (dedup hit -&gt; republish) are the
 * same as after a real pod restart; the process lifecycle is not.
 *
 * <h2>What each assertion would catch</h2>
 *
 * <ul>
 *   <li>Zero events after step 3 -&gt; the pre-drill-9 "skip on dedup" behaviour: permanent loss.
 *   <li>Two events -&gt; the republish is not idempotent from the consumer's point of view.
 *   <li>An event whose envelope {@code eventId} differs from the row's {@code status_event_id}
 *       -&gt; a freshly minted id, which every downstream dedup key would treat as new work
 *       (the ledger would double-count the payment).
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CrashRedeliveryIT extends PspConnectorIntegrationSupport {

    private static final Logger log = LoggerFactory.getLogger(CrashRedeliveryIT.class);

    private static final String REQUESTED_TOPIC = "payments.payment-requested.v1.crash-it";

    private static final String STATUS_TOPIC = "payments.payment-status-changed.v1.crash-it";

    private static final String MERCHANT_ID = "merchant-crash-it";

    /**
     * Static so the test method can flip it: the decorator bean lives in the Spring context, the
     * test drives the scenario. See the class javadoc for why this is a flag and not a counter.
     */
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
            assertThat(beforeRecovery)
                    .as(
                            "the attempt row is committed but the publish keeps failing - this is exactly "
                                    + "the drill-9 window, and nothing may be on the topic yet")
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
            // Keep polling: the assertion is EXACTLY one event, so a spurious second one has to be
            // given a fair chance to arrive rather than be excluded by stopping at the first hit.
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

            // The generalised form of the same rule, and the reason TIMEOUT is excluded: ADR-0006
            // category A never produces a status event at all (M12's provider-status query is the
            // exit for those), so only non-TIMEOUT rows are owed one.
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

    /** Waits for the {@code payment_attempts} insert, i.e. for the loss window to be open. */
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

    /**
     * Opens the drill-9 window: {@code publishStatusChanged} throws for as long as the test holds
     * {@link #PUBLISH_FAILING}, then delegates to the real Kafka publisher. Test scope only - the
     * production wiring has no such decorator, and the real publisher still performs the send that
     * the assertions read back off the broker.
     */
    @TestConfiguration
    static class FailFirstPublishConfiguration {

        @Bean
        @Primary
        PaymentStatusPublisher failingPaymentStatusPublisher(KafkaPaymentStatusPublisher delegate) {
            return new PaymentStatusPublisher() {
                @Override
                public void publishStatusChanged(PaymentAttempt attempt) {
                    if (PUBLISH_FAILING.get()) {
                        // KafkaException is what the real publisher throws when the broker never
                        // acknowledges the send, so the container's error handler sees the same
                        // type it would in production.
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
