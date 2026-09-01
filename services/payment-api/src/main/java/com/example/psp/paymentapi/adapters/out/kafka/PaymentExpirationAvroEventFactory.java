package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Payment;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code payments.payment-status-changed.v1}'s {@code EXPIRED}
 * case (M22) - a plain method, not a MapStruct {@code @Mapper}, same established exception every
 * {@code *AvroEventFactory} in this codebase uses (see {@code MerchantConfigAvroEventFactory}'s
 * javadoc for the full reasoning).
 *
 * <p>{@code providerReference} is always {@code ""} on the wire, same sentinel psp-connector's own
 * PENDING publish uses ({@code KafkaPaymentStatusAvroEventFactory#toNonTerminalAvro}) - EXPIRED is
 * this service's own conclusion, not a provider's; there is no provider event id to carry.
 */
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
                        // No inbound event caused this - a root cause in its own right, same as
                        // ledger's TTL sweep (SweepExpiredReservationsUseCase). traceId/
                        // correlationId are freshly minted for the same reason: nothing upstream
                        // to propagate from.
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
