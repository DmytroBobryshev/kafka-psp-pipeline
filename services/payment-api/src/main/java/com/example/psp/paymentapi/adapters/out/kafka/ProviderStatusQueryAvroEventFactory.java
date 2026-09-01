package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import org.springframework.stereotype.Component;

@Component
public class ProviderStatusQueryAvroEventFactory {

    public com.example.psp.common.events.avro.ProviderStatusQuery toAvro(
            EventEnvelope envelope, String paymentId, String merchantId) {
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

        return com.example.psp.common.events.avro.ProviderStatusQuery.newBuilder()
                .setEnvelope(avroEnvelope)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .build();
    }
}
