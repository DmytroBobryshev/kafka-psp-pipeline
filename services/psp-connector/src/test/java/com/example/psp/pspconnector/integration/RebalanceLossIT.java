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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RebalanceLossIT extends PspConnectorIntegrationSupport {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLossIT.class);

    private static final String REQUESTED_TOPIC = "payments.payment-requested.v1.rebalance-it";

    private static final String STATUS_TOPIC = "payments.payment-status-changed.v1.rebalance-it";

    private static final int REQUESTED_PARTITIONS = 3;

    private static final int PAYMENT_COUNT = 30;

    private static final int REPLAYED_COUNT = 5;

    private static final String MERCHANT_ID = "merchant-rebalance-it";

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

            List<ConsumerRecord<String, PaymentStatusChanged>> replayEvents =
                    drainUntil(
                            verifier,
                            STATUS_TOPIC,
                            Duration.ofSeconds(60),
                            records -> terminalOnly(records).size() >= REPLAYED_COUNT);
            List<ConsumerRecord<String, PaymentStatusChanged>> all =
                    Stream.concat(afterRebalances.stream(), replayEvents.stream()).toList();
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
