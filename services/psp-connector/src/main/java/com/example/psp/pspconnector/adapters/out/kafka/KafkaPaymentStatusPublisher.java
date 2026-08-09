package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link PaymentStatusPublisher}. Publishes
 * {@code payments.payment-status-changed.v1}, keyed by {@code merchantId}.
 *
 * <h2>Why this topic's key differs from the inbound topic's key</h2>
 *
 * {@code payments.payment-requested.v1} (what this service <b>consumes</b>) is keyed by
 * {@code paymentId}; this topic (what this service <b>produces</b>) is keyed by
 * {@code merchantId}. That asymmetry is deliberate, not an inconsistency (ADR-0003):
 *
 * <ul>
 *   <li>{@code payment-requested} carries exactly one event per payment - there is nothing to
 *       order on a single-event-per-aggregate topic, so its key is chosen purely to spread load
 *       evenly across psp-connector's 12 partitions (the slowest step in the pipeline, 100 ms-5 s
 *       per call).
 *   <li>{@code payment-status-changed} can carry <b>multiple</b> events for the same payment over
 *       time (M11's refund saga adds more), and every downstream consumer that needs per-merchant
 *       ordering - the ledger's single-writer-per-balance invariant (M7) above all - depends on
 *       every status change for a given merchant landing on the same partition, in order. Keying
 *       by {@code paymentId} here would let two status changes for the same merchant race across
 *       partitions with no ordering guarantee between them.
 * </ul>
 *
 * <p>In short: {@code paymentId} on the inbound topic buys balance; {@code merchantId} on this
 * outbound topic buys the ordering the ledger's exactly-once bookkeeping (M7) requires. Picking
 * the same key for both would either serialize psp-connector's slow provider calls behind one
 * partition per merchant (rejected in ADR-0003's "Alternatives considered"), or cost the ledger
 * its single-writer guarantee - there is no single key that is right for both topics.
 */
@Component
public class KafkaPaymentStatusPublisher implements PaymentStatusPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPaymentStatusPublisher.class);
    private static final String EVENT_TYPE = "payments.payment-status-changed.v1";
    private static final String SOURCE = "psp-connector";
    private static final String AGGREGATE_TYPE = "payment";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentStatusEventMapper eventMapper;
    private final String topic;

    public KafkaPaymentStatusPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            PaymentStatusEventMapper eventMapper,
            @Value("${psp-connector.kafka.payment-status-changed-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventMapper = eventMapper;
        this.topic = topic;
    }

    @Override
    public void publishStatusChanged(PaymentAttempt attempt) {
        if (attempt.getOutcome() == ProviderOutcome.TIMEOUT) {
            // Defensive: application/ already guarantees this never happens (ADR-0006 category A
            // is never published) - this is a second line of defence against a future caller
            // mistake, not a path this code expects to take.
            throw new IllegalStateException(
                    "must never publish payments.payment-status-changed.v1 for a TIMEOUT outcome (ADR-0006), paymentId="
                            + attempt.getPaymentId());
        }

        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        attempt.getCausationEventId(),
                        EVENT_TYPE,
                        1,
                        attempt.getPaymentId().toString(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        attempt.getTraceId(),
                        attempt.getCorrelationId());
        PaymentStatusChanged event = eventMapper.toEvent(envelope, attempt);

        // Key = merchantId, NOT paymentId - see this class's javadoc.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, attempt.getMerchantId(), event);
        record.headers()
                .add("traceparent", attempt.getTraceId().getBytes(StandardCharsets.UTF_8))
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

        kafkaTemplate
                .send(record)
                .whenComplete(
                        (result, ex) -> {
                            if (ex != null) {
                                log.error(
                                        "Failed to publish {} for paymentId={}",
                                        topic,
                                        attempt.getPaymentId(),
                                        ex);
                            } else {
                                RecordMetadata metadata = result.getRecordMetadata();
                                log.info(
                                        "Published {} paymentId={} merchantId={} status={} partition={} offset={}",
                                        topic,
                                        attempt.getPaymentId(),
                                        attempt.getMerchantId(),
                                        event.status(),
                                        metadata.partition(),
                                        metadata.offset());
                            }
                        });
    }
}
