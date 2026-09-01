package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import org.springframework.stereotype.Component;

@Component
public class RefundStatusAvroEventFactory {

    private static com.example.psp.common.events.avro.EventEnvelope toAvroEnvelope(EventEnvelope envelope) {
        return com.example.psp.common.events.avro.EventEnvelope.newBuilder()
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
    }

    public com.example.psp.common.events.avro.RefundCompleted toRefundCompleted(
            EventEnvelope envelope, RefundAttempt attempt) {
        return com.example.psp.common.events.avro.RefundCompleted.newBuilder()
                .setEnvelope(toAvroEnvelope(envelope))
                .setRefundId(attempt.getRefundId().toString())
                .setPaymentId(attempt.getPaymentId().toString())
                .setMerchantId(attempt.getMerchantId())
                .setAmount(attempt.getAmount().amount())
                .setCurrency(attempt.getAmount().currency())
                .setProviderReference(attempt.getProviderReference().toString())
                .build();
    }

    public com.example.psp.common.events.avro.RefundFailed toRefundFailed(
            EventEnvelope envelope, RefundAttempt attempt) {
        return com.example.psp.common.events.avro.RefundFailed.newBuilder()
                .setEnvelope(toAvroEnvelope(envelope))
                .setRefundId(attempt.getRefundId().toString())
                .setPaymentId(attempt.getPaymentId().toString())
                .setMerchantId(attempt.getMerchantId())
                .setAmount(attempt.getAmount().amount())
                .setCurrency(attempt.getAmount().currency())
                .setReason("PROVIDER_DECLINED")
                .build();
    }

    public com.example.psp.common.events.avro.RefundStatusChanged toNonTerminalAvro(
            EventEnvelope envelope,
            java.util.UUID refundId,
            java.util.UUID paymentId,
            String merchantId,
            com.example.psp.pspconnector.domain.model.Money amount,
            String status,
            String providerReference) {
        return com.example.psp.common.events.avro.RefundStatusChanged.newBuilder()
                .setEnvelope(toAvroEnvelope(envelope))
                .setRefundId(refundId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId)
                .setAmount(amount.amount())
                .setCurrency(amount.currency())
                .setStatus(status)
                .setProviderReference(providerReference)
                .build();
    }
}
