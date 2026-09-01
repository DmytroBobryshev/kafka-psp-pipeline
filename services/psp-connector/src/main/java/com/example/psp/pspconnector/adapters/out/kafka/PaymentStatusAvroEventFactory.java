package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code payments.payment-status-changed.v1} (M9 Phase 2) from the
 * hand-written {@link EventEnvelope} and the {@link PaymentAttempt} aggregate.
 *
 * <p>A plain method, not a MapStruct {@code @Mapper} - the same deliberate exception to this
 * codebase's "MapStruct at every boundary" convention that
 * {@code payment-api}'s {@code adapters.out.outbox.PaymentAvroEventFactory} established in M9
 * Phase 1 (see that class's javadoc for the full reasoning: MapStruct's builder-pattern support is
 * not exercised anywhere else in this codebase, and a five/six-field flat record reached through
 * the generated {@code newBuilder()} is not worth an annotation-processed interface). Kept
 * consistent with that precedent rather than re-litigating it per topic.
 */
@Component
public class PaymentStatusAvroEventFactory {

    /**
     * Builder for every non-terminal status this service emits before the terminal SUCCEEDED/
     * DECLINED publish: PENDING (providerReference {@code ""}, no provider call yet), IPN_RECEIVED
     * and VERIFIED (both carry the provider's own event id as {@code providerReference}). One
     * generalized method rather than three near-identical ones - {@code toPendingAvro} was this
     * method under a stage-specific name before M21 added the other two stages.
     *
     * @param envelope         the JSON-era {@link EventEnvelope} this service already builds for
     *                         every outbound event (unchanged - {@link KafkaPaymentStatusPublisher}
     *                         still owns causation/trace/correlation wiring).
     * @param status           {@code "PENDING"}, {@code "IPN_RECEIVED"}, or {@code "VERIFIED"}.
     * @param providerReference the provider's event id, or {@code ""} for PENDING (no provider call
     *                          has happened yet).
     * @return the Avro record ready to hand to {@code KafkaTemplate#send}.
     */
    public com.example.psp.common.events.avro.PaymentStatusChanged toNonTerminalAvro(
            com.example.psp.common.events.EventEnvelope envelope,
            java.util.UUID paymentId,
            String merchantId,
            com.example.psp.pspconnector.domain.model.Money amount,
            String status,
            String providerReference) {
        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(envelope.eventId().toString())
                        .setEventType(envelope.eventType())
                        .setEventVersion(envelope.eventVersion())
                        .setAggregateId(envelope.aggregateId())
                        .setAggregateType(envelope.aggregateType())
                        .setOccurredAt(envelope.occurredAt())
                        .setSource(envelope.source())
                        .setTraceId(envelope.traceId())
                        .setCorrelationId(envelope.correlationId())
                        .setCausationId(envelope.causationId() == null ? null : envelope.causationId().toString())
                        .build();
        return com.example.psp.common.events.avro.PaymentStatusChanged.newBuilder()
                .setEnvelope(avroEnvelope)
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId)
                .setAmount(amount.amount())
                .setCurrency(amount.currency())
                .setStatus(status)
                .setProviderReference(providerReference)
                .setDeclineReason(null)
                .build();
    }

    public com.example.psp.common.events.avro.PaymentStatusChanged toAvro(
            EventEnvelope envelope, PaymentAttempt attempt) {
        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(envelope.eventId().toString())
                        .setEventType(envelope.eventType())
                        .setEventVersion(envelope.eventVersion())
                        .setAggregateId(envelope.aggregateId())
                        .setAggregateType(envelope.aggregateType())
                        .setOccurredAt(envelope.occurredAt())
                        .setSource(envelope.source())
                        .setTraceId(envelope.traceId())
                        .setCorrelationId(envelope.correlationId())
                        .setCausationId(envelope.causationId() == null ? null : envelope.causationId().toString())
                        .build();

        boolean declined = attempt.getOutcome() == ProviderOutcome.DECLINED;

        return com.example.psp.common.events.avro.PaymentStatusChanged.newBuilder()
                .setEnvelope(avroEnvelope)
                .setPaymentId(attempt.getPaymentId().toString())
                .setMerchantId(attempt.getMerchantId())
                .setAmount(attempt.getAmount().amount())
                .setCurrency(attempt.getAmount().currency())
                .setStatus(attempt.getOutcome() == ProviderOutcome.APPROVED ? "SUCCEEDED" : "DECLINED")
                .setProviderReference(attempt.getProviderEventId().toString())
                .setDeclineReason(declined ? "simulated decline" : null)
                .build();
    }
}
