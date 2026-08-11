package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.Money;
import java.util.UUID;

/**
 * Application-layer input for {@link SettleRefundUseCase} - built from the inbound
 * {@code refunds.refund-completed.v1} event.
 *
 * @param inboundEventId the inbound envelope's own {@code eventId} - both the dedup key for this
 *                       specific consumption and the {@code causationId} the resulting
 *                       {@code ledger.ledger-entry-recorded.v1} entry carries.
 * @param refundId       the saga's correlation id.
 * @param paymentId      the payment that was refunded.
 * @param merchantId     the merchant whose reservation is being settled.
 * @param amount         the settled amount (must equal the amount reserved; not re-validated here -
 *                       the reservation, not this event, is authoritative for how much money moved).
 * @param providerReference the (simulated) provider's reference for this refund - carried for audit
 *                       only; not part of any idempotency key.
 * @param traceId        propagated from the inbound envelope.
 * @param correlationId  propagated from the inbound envelope.
 */
public record SettleRefundCommand(
        UUID inboundEventId,
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        String providerReference,
        String traceId,
        String correlationId) {
}
