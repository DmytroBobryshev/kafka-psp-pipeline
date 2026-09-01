package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Payment;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class PaymentExpirationAvroEventFactory {

    private static final String STATUS = "EXPIRED";
    private static final String PROVIDER_REFERENCE_SENTINEL = "";

    public com.example.psp.common.events.avro.PaymentStatusChanged toAvro(
            UUID eventId, Payment payment, Instant occurredAt) {

        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(eventId.toString())
                        .setEventType("payments.payment-status-changed.v1")
                        .setEventVersion(1)
                        .setAggregateId(payment.getId().toString())
                        .setAggregateType("payment")
                        .setOccurredAt(occurredAt)
                        .setSource("payment-api")
                        .setTraceId(UUID.randomUUID().toString())
                        .setCorrelationId(UUID.randomUUID().toString())
                        .setCausationId(null)
                        .build();

        return com.example.psp.common.events.avro.PaymentStatusChanged.newBuilder()
                .setEnvelope(avroEnvelope)
                .setPaymentId(payment.getId().toString())
                .setMerchantId(payment.getMerchantId())
                .setAmount(payment.getAmount().amount())
                .setCurrency(payment.getAmount().currency())
                .setStatus(STATUS)
                .setProviderReference(PROVIDER_REFERENCE_SENTINEL)
                .setDeclineReason(null)
                .build();
    }
}
