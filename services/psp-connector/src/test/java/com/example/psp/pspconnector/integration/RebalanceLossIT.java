package com.example.psp.pspconnector.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * M19 "Plus", the flagship: <b>no payment is lost, and no payment is double-counted, across two
 * consumer-group rebalances taken while events are in flight.</b>
 *
 * <p>docs/PLAN.md's M19 "Plus" line is the brief - "a test asserting correct behaviour across a
 * rebalance teaches more than any article". Everything here is real: a real KRaft broker
 * (Testcontainers {@code apache/kafka}), a real Postgres with Flyway V1-V4 applied, the real
 * {@code PaymentRequestedListener} with its real manual-ack container, the real
 * {@code ProcessPaymentRequestUseCase}, the real {@code KafkaPaymentStatusPublisher}. The only
 * simulated thing is the payment provider, which is simulated in production too.
 *
 * <h2>What "force a rebalance" means mechanically</h2>
 *
 * <p>{@link MessageListenerContainer#stop()} makes the container's consumer send a LeaveGroup and
 * shut down; {@link MessageListenerContainer#start()} makes a fresh consumer JoinGroup and get the
 * partitions assigned again. That is a genuine group rebalance driven by the broker's group
 * coordinator - the same event a Kubernetes rolling restart or a KEDA scale-in causes, which is
 * precisely what M19 drill 9 was about. It is done twice, mid-stream, with records still
 * unprocessed and offsets still uncommitted.
 *
 * <h2>The two assertions, and why each one has teeth</h2>
 *
 * <ol>
 *   <li><b>No loss.</b> All {@value #PAYMENT_COUNT} payments must appear on
 *       {@code payment-status-changed}. A rebalance that dropped an in-flight batch without
 *       redelivering it - or a listener that acked before the publish was broker-acknowledged, the
 *       actual drill-9 defect - shows up here as a missing paymentId. Note that this assertion is
 *       only exact because {@code psp-connector.provider.forced-outcome=APPROVED} removes the
 *       simulator's 5% TIMEOUT dice: a TIMEOUT is ADR-0006 category A and deliberately publishes
 *       NOTHING, so with the dice left in, "one status event per payment" would simply not be the
 *       correct expectation.
 *   <li><b>Duplicates are permitted, but only under one identity.</b> At-least-once delivery means
 *       a redelivered record may legitimately produce a second status event - the system does not
 *       promise the consumer sees each record once. What it does promise is that the second copy is
 *       recognisably the same event: {@code KafkaPaymentStatusPublisher} republishes under the
 *       attempt row's stored {@code statusEventId} (V4 column) instead of minting a fresh
 *       {@code eventId}, so downstream idempotency keyed on the envelope id (the ledger's
 *       {@code uq_ledger_entries_inbound_event_id}) drops the copy. So the test groups the status
 *       events by paymentId and asserts each group has exactly ONE distinct envelope eventId.
 *       Delete {@code ProcessPaymentRequestUseCase#republish}'s use of the stored id and this
 *       assertion fails; delete the republish call entirely and assertion 1 fails.
 * </ol>
 *
 * <p>To make assertion 2 test something rather than pass vacuously, the test does not rely on a
 * rebalance happening to produce a duplicate (it might not - MANUAL_IMMEDIATE acks per record, so a
 * clean stop can leave no redelivery at all). It re-sends {@value #REPLAYED_COUNT} of the original
 * records verbatim afterwards, the same thing an operator resetting a consumer group's offsets to
 * earliest does. Those are guaranteed to hit M5 level 1 and take the republish path.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RebalanceLossIT extends PspConnectorIntegrationSupport {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLossIT.class);

    /** Per-IT topics - see {@link PspConnectorIntegrationSupport}'s "do not interfere" note. */
    private static final String REQUESTED_TOPIC = "payments.payment-requested.v1.rebalance-it";

    private static final String STATUS_TOPIC = "payments.payment-status-changed.v1.rebalance-it";

    /** More than one so the rebalance actually has partitions to move around. */
    private static final int REQUESTED_PARTITIONS = 3;

    private static final int PAYMENT_COUNT = 30;

    private static final int REPLAYED_COUNT = 5;

    private static final String MERCHANT_ID = "merchant-rebalance-it";

    /**
     * 150 ms per authorize() call. Not cosmetic: it stretches {@value #PAYMENT_COUNT} records into
     * ~4.5 s of processing, which is what gives the two bounces below something to interrupt. At the
     * unit-test profile's 0-1 ms the whole stream would be drained before the first {@code stop()}
     * and the test would prove nothing about a rebalance.
     */
    private static final int FORCED_LATENCY_MS = 150;

    @Autowired private JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void createIntegrationTopics() {
        createTopics(Map.of(REQUESTED_TOPIC, REQUESTED_PARTITIONS, STATUS_TOPIC, 1));
    }

    @DynamicPropertySource
    static void rebalanceItProperties(DynamicPropertyRegistry registry) {
        registry.add("psp-connector.kafka.payment-requested-topic", () -> REQUESTED_TOPIC);
        registry.add("psp-connector.kafka.payment-status-changed-topic", () -> STATUS_TOPIC);
        registry.add("spring.kafka.consumer.group-id", () -> "psp-connector.rebalance-it.v1");
        registry.add("psp-connector.provider.forced-latency-ms", () -> FORCED_LATENCY_MS);
    }

    @Test
    void everyPaymentSurvivesTwoRebalancesAndDuplicatesShareOneEventId() throws Exception {
        MessageListenerContainer listener = listenerContainerFor(REQUESTED_TOPIC);
        ContainerTestUtils.waitForAssignment(listener, REQUESTED_PARTITIONS);

        Map<UUID, UUID> paymentIdByEventId = new LinkedHashMap<>();
        List<UUID> eventIds = new ArrayList<>();

        try (Producer<String, Object> producer = avroProducer();
                KafkaConsumer<String, PaymentStatusChanged> verifier = statusChangedConsumer()) {

            for (int i = 0; i < PAYMENT_COUNT; i++) {
                UUID eventId = UUID.randomUUID();
                UUID paymentId = UUID.randomUUID();
                eventIds.add(eventId);
                paymentIdByEventId.put(eventId, paymentId);
                send(
                        producer,
                        REQUESTED_TOPIC,
                        paymentId.toString(),
                        paymentRequested(eventId, paymentId, MERCHANT_ID, new BigDecimal("10.00")));
            }
            producer.flush();

            // --- the drill itself: two rebalances while the stream is still being processed -----
            bounce(listener, 1);
            bounce(listener, 2);

            // M21: PENDING/IPN_RECEIVED/VERIFIED now precede every payment's terminal event on this
            // same topic, so both the drain predicate and the "no payment lost" check below must
            // count the TERMINAL event specifically - counting any record's paymentId would let the
            // drain stop (and this assertion pass) the moment every payment's PENDING has arrived,
            // long before most of them have actually reached SUCCEEDED.
            List<ConsumerRecord<String, PaymentStatusChanged>> afterRebalances =
                    drainUntil(
                            verifier,
                            STATUS_TOPIC,
                            Duration.ofSeconds(90),
                            records -> distinctPaymentIds(terminalOnly(records)).size() >= PAYMENT_COUNT);

            assertThat(distinctPaymentIds(terminalOnly(afterRebalances)))
                    .as(
                            "no payment may be lost across two rebalances - %d/%d terminal status events "
                                    + "arrived",
                            distinctPaymentIds(terminalOnly(afterRebalances)).size(), PAYMENT_COUNT)
                    .containsExactlyInAnyOrderElementsOf(
                            paymentIdByEventId.values().stream().map(UUID::toString).toList());

            // --- guarantee the republish path is exercised, rather than hoping for it ----------
            for (int i = 0; i < REPLAYED_COUNT; i++) {
                UUID eventId = eventIds.get(i);
                UUID paymentId = paymentIdByEventId.get(eventId);
                send(
                        producer,
                        REQUESTED_TOPIC,
                        paymentId.toString(),
                        paymentRequested(eventId, paymentId, MERCHANT_ID, new BigDecimal("10.00")));
            }
            producer.flush();

            // drainUntil returns what THIS call collected (the consumer keeps its position), so the
            // two phases are concatenated rather than the second one replacing the first.
            List<ConsumerRecord<String, PaymentStatusChanged>> replayEvents =
                    drainUntil(
                            verifier,
                            STATUS_TOPIC,
                            Duration.ofSeconds(60),
                            records -> terminalOnly(records).size() >= REPLAYED_COUNT);
            List<ConsumerRecord<String, PaymentStatusChanged>> all =
                    Stream.concat(afterRebalances.stream(), replayEvents.stream()).toList();
            // The republish() path (level 1) sits before authorize() is ever called again, so a
            // replay re-emits ONLY the terminal event - never PENDING/IPN_RECEIVED/VERIFIED (see
            // ProcessPaymentRequestUseCase). The "no double-count" accounting below is therefore
            // about the terminal event specifically; filter once and reuse everywhere.
            List<ConsumerRecord<String, PaymentStatusChanged>> terminalEvents = terminalOnly(all);

            log.info(
                    "RebalanceLossIT saw {} total status events ({} terminal) for {} payments "
                            + "({} duplicate terminal deliveries)",
                    all.size(),
                    terminalEvents.size(),
                    distinctPaymentIds(terminalEvents).size(),
                    terminalEvents.size() - distinctPaymentIds(terminalEvents).size());

            Map<String, List<ConsumerRecord<String, PaymentStatusChanged>>> byPaymentId =
                    terminalEvents.stream()
                            .collect(Collectors.groupingBy(record -> record.value().getPaymentId()));

            assertThat(byPaymentId.keySet())
                    .as("still no payment lost after the deliberate replay")
                    .hasSize(PAYMENT_COUNT);

            assertThat(terminalEvents.size() - byPaymentId.size())
                    .as(
                            "the %d verbatim replays must each have produced a REPUBLISHED terminal status "
                                    + "event (M19 drill 9) - if this is 0 the republish path never ran and "
                                    + "the identity assertion below would be vacuous",
                            REPLAYED_COUNT)
                    .isGreaterThanOrEqualTo(REPLAYED_COUNT);

            byPaymentId.forEach(
                    (paymentId, records) -> {
                        Set<String> envelopeEventIds =
                                records.stream()
                                        .map(record -> record.value().getEnvelope().getEventId())
                                        .collect(Collectors.toSet());
                        assertThat(envelopeEventIds)
                                .as(
                                        "paymentId=%s produced %d terminal status events; duplicates are "
                                                + "allowed ONLY under one envelope eventId (the attempt row's "
                                                + "stored statusEventId), otherwise downstream dedup cannot see "
                                                + "them as the same event",
                                        paymentId, records.size())
                                .hasSize(1);
                    });

            assertThat(terminalEvents)
                    .as("forced-outcome=APPROVED must map to SUCCEEDED on every terminal status event")
                    .allMatch(record -> "SUCCEEDED".equals(record.value().getStatus()));
        }

        // The database side of "no loss, no double-count": M5 level 1 keyed on the inbound
        // envelope eventId means the 5 replays add no rows at all.
        Integer attemptRows =
                jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM payment_attempts WHERE merchant_id = ?",
                        Integer.class,
                        MERCHANT_ID);
        assertThat(attemptRows)
                .as("one payment_attempts row per distinct inbound eventId, replays included")
                .isEqualTo(PAYMENT_COUNT);

        Integer withoutStatusEventId =
                jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM payment_attempts WHERE merchant_id = ? AND status_event_id IS NULL",
                        Integer.class,
                        MERCHANT_ID);
        assertThat(withoutStatusEventId)
                .as("V4's status_event_id is what makes a republish byte-identical - it must be set")
                .isZero();
    }

    /**
     * One full stop/start cycle of the listener container: LeaveGroup, then JoinGroup and wait for
     * the partitions to come back. The sleep before it is what puts the bounce mid-stream rather
     * than after the backlog has already drained.
     */
    private void bounce(MessageListenerContainer listener, int round) throws InterruptedException {
        Thread.sleep(900);
        log.info("--- rebalance {} : stopping listener container ---", round);
        listener.stop();
        log.info("--- rebalance {} : restarting listener container ---", round);
        listener.start();
        ContainerTestUtils.waitForAssignment(listener, REQUESTED_PARTITIONS);
        log.info("--- rebalance {} : partitions reassigned ---", round);
    }

    private static List<String> distinctPaymentIds(
            List<ConsumerRecord<String, PaymentStatusChanged>> records) {
        return records.stream().map(record -> record.value().getPaymentId()).distinct().toList();
    }
}
