package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.common.events.UuidV7;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
    private final PaymentStatusAvroEventFactory avroEventFactory;
    private final String topic;

    public KafkaPaymentStatusPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            PaymentStatusAvroEventFactory avroEventFactory,
            @Value("${psp-connector.kafka.payment-status-changed-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
    }

    @Override
    public void publishPending(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        publishNonTerminal(
                "PENDING", paymentId, merchantId, amount, "", causationEventId, traceId, correlationId);
    }

    @Override
    public void publishIpnReceived(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        publishNonTerminal(
                "IPN_RECEIVED",
                paymentId,
                merchantId,
                amount,
                providerReference.toString(),
                causationEventId,
                traceId,
                correlationId);
    }

    @Override
    public void publishVerified(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        publishNonTerminal(
                "VERIFIED",
                paymentId,
                merchantId,
                amount,
                providerReference.toString(),
                causationEventId,
                traceId,
                correlationId);
    }

    /**
     * Shared shape for every non-terminal status (PENDING/IPN_RECEIVED/VERIFIED): fresh eventId per
     * emission, blocking send - same as {@link #publishStatusChanged}'s blocking-send discipline,
     * minus the stored-eventId republish logic that only applies to the terminal event.
     */
    private void publishNonTerminal(
            String status,
            UUID paymentId,
            String merchantId,
            Money amount,
            String providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        causationEventId, EVENT_TYPE, 1, paymentId.toString(), AGGREGATE_TYPE, SOURCE,
                        traceId, correlationId);
        var event =
                avroEventFactory.toNonTerminalAvro(
                        envelope, paymentId, merchantId, amount, status, providerReference);
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, merchantId, event);
        record.headers()
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("interrupted while publishing " + status + " for paymentId=" + paymentId, e);
        } catch (ExecutionException e) {
            throw new KafkaException("failed to publish " + status + " for paymentId=" + paymentId, e.getCause());
        }
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

        // The envelope eventId is the attempt's stored statusEventId, NOT a fresh mint: a
        // redelivered inbound event republishes through here (M19 drill 9), and downstream dedup
        // (the ledger's uq_ledger_entries_inbound_event_id above all) only recognises the replay
        // if the id is byte-identical. Fresh mint is the pre-V4-row fallback only.
        UUID eventId = attempt.getStatusEventId() != null ? attempt.getStatusEventId() : UuidV7.generate();
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        eventId,
                        attempt.getCausationEventId(),
                        EVENT_TYPE,
                        1,
                        attempt.getPaymentId().toString(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        attempt.getTraceId(),
                        attempt.getCorrelationId());
        com.example.psp.common.events.avro.PaymentStatusChanged event = avroEventFactory.toAvro(envelope, attempt);

        // Key = merchantId, NOT paymentId - see this class's javadoc.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, attempt.getMerchantId(), event);
        // M15: no hand-written "traceparent" header here anymore - KafkaTemplate's observation
        // (enabled in config.KafkaProducerConfig) injects a real W3C one on send(), continuing the
        // SAME trace this consumer's own inbound record carried. See ADR-0002/M15 reconciliation
        // in infra/compose/README.md for why the header is now owned exclusively by Micrometer's
        // instrumentation while envelope.traceId() (set above from attempt.getTraceId(), itself
        // forwarded from the consumed event's envelope) stays the value-level record of the SAME
        // trace id for anything reading the deserialized record without headers (AKHQ, DLQ dumps).
        record.headers()
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

        // Blocks until the broker acknowledges (acks=all) or the producer gives up
        // (delivery.timeout.ms, well under max.poll.interval.ms). The fire-and-forget send this
        // replaced was M19 drill 9's loss: the listener acked the inbound offset while this
        // record still sat in the producer buffer, and a KEDA scale-in killed the pod inside
        // that window. A failure must reach the listener so the offset is never committed ahead
        // of the event (ADR-0006's error handler takes it from there).
        try {
            SendResult<String, Object> result = kafkaTemplate.send(record).get();
            RecordMetadata metadata = result.getRecordMetadata();
            log.info(
                    "Published {} paymentId={} merchantId={} status={} eventId={} partition={} offset={}",
                    topic,
                    attempt.getPaymentId(),
                    attempt.getMerchantId(),
                    event.getStatus(),
                    eventId,
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException(
                    "interrupted while publishing " + topic + " for paymentId=" + attempt.getPaymentId(), e);
        } catch (ExecutionException e) {
            throw new KafkaException(
                    "failed to publish " + topic + " for paymentId=" + attempt.getPaymentId(), e.getCause());
        }
    }
}
