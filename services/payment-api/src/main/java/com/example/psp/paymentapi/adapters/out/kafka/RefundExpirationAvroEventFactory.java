package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Refund;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class RefundExpirationAvroEventFactory {

    private static final String STATUS = "EXPIRED";
    private static final String PROVIDER_REFERENCE_SENTINEL = "";

    public com.example.psp.common.events.avro.RefundStatusChanged toAvro(
            UUID eventId, Refund refund, Instant occurredAt) {

        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(eventId.toString())
                        .setEventType("refunds.refund-status-changed.v1")
                        .setEventVersion(1)
                        .setAggregateId(refund.getId().toString())
                        .setAggregateType("refund")
                        .setOccurredAt(occurredAt)
                        .setSource("payment-api")
                        .setTraceId(UUID.randomUUID().toString())
                        .setCorrelationId(UUID.randomUUID().toString())
                        .setCausationId(null)
                        .build();

        return com.example.psp.common.events.avro.RefundStatusChanged.newBuilder()
                .setEnvelope(avroEnvelope)
                .setRefundId(refund.getId().toString())
                .setPaymentId(refund.getPaymentId().toString())
                .setMerchantId(refund.getMerchantId())
                .setAmount(refund.getAmount().amount())
                .setCurrency(refund.getAmount().currency())
                .setStatus(STATUS)
                .setProviderReference(PROVIDER_REFERENCE_SENTINEL)
                .build();
    }
}
