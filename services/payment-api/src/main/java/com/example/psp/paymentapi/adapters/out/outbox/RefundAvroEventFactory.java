package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Refund;
import org.springframework.stereotype.Component;

@Component
public class RefundAvroEventFactory {

    public com.example.psp.common.events.avro.RefundRequested toAvro(EventEnvelope envelope, Refund refund) {
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

        return com.example.psp.common.events.avro.RefundRequested.newBuilder()
                .setEnvelope(avroEnvelope)
                .setRefundId(refund.getId().toString())
                .setPaymentId(refund.getPaymentId().toString())
                .setMerchantId(refund.getMerchantId())
                .setAmount(refund.getAmount().amount())
                .setCurrency(refund.getAmount().currency())
                .setReason(refund.getReason())
                .build();
    }
}
