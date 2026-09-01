package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import org.springframework.stereotype.Component;

@Component
public class PaymentAvroEventFactory {

    public com.example.psp.common.events.avro.PaymentRequested toAvro(EventEnvelope envelope, Payment payment) {
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

        return com.example.psp.common.events.avro.PaymentRequested.newBuilder()
                .setEnvelope(avroEnvelope)
                .setPaymentId(payment.getId().toString())
                .setMerchantId(payment.getMerchantId())
                .setAmount(payment.getAmount().amount())
                .setCurrency(payment.getAmount().currency())
                .setStatus(payment.getStatus().name())
                .build();
    }
}
