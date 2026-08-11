package com.example.psp.ledger.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * The ledger's local view of one refund's saga state (M11, ADR-0008 rule 1). Pure Java, no
 * framework dependency (ADR-0007) - both the internal read model {@code
 * domain.port.RefundRepository#findSagaState} returns and the source the web boundary maps into
 * {@code GET /api/refunds/{refundId}}'s response.
 *
 * @param refundId   the saga's correlation id throughout (envelope.aggregateId on every
 *                   refunds.*.v1 event).
 * @param paymentId  the payment being refunded.
 * @param merchantId the owning merchant.
 * @param amount     the refund amount.
 * @param status     current state - see {@link RefundSagaStatus}.
 * @param reason     populated for {@link RefundSagaStatus#FAILED}, {@link RefundSagaStatus#RELEASED}
 *                   and {@link RefundSagaStatus#NEEDS_MANUAL_REVIEW}; {@code null} otherwise.
 * @param createdAt  when this saga's row was first written (at RESERVED or FAILED).
 * @param updatedAt  when {@code status} last changed - what the TTL sweeper compares against
 *                   {@code refund.reservation.ttl} for rows still {@link RefundSagaStatus#RESERVED}.
 */
public record RefundSagaState(
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        RefundSagaStatus status,
        String reason,
        Instant createdAt,
        Instant updatedAt) {

    public RefundSagaState {
        Objects.requireNonNull(refundId, "refundId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}
