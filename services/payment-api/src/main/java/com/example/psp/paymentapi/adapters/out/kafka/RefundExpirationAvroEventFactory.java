package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Refund;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code refunds.refund-status-changed.v1}'s {@code EXPIRED} case
 * (M24) - the refund-path mirror of {@code PaymentExpirationAvroEventFactory}. A plain method, not
 * a MapStruct {@code @Mapper}, same established exception every {@code *AvroEventFactory} in this
 * codebase uses.
 *
 * <p>{@code providerReference} is always {@code ""} on the wire, same sentinel PENDING already
 * uses on this same topic ({@code RefundStatusChanged}'s schema doc) - EXPIRED is this service's
 * own conclusion, not a provider's; there is no provider event id to carry.
 */
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
                        // No inbound event caused this - a root cause in its own right, same as
                        // ExpirePaymentsUseCase's identical publish and ledger's TTL sweep.
                        // traceId/correlationId are freshly minted for the same reason: nothing
                        // upstream to propagate from.
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
