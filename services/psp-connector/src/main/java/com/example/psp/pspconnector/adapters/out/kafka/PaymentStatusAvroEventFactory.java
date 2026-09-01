package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import org.springframework.stereotype.Component;

@Component
public class PaymentStatusAvroEventFactory {

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
