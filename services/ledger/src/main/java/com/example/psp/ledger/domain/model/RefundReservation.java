package com.example.psp.ledger.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * An immutable audit record of one successful refund reservation (M11) - "a reservation row" per
 * the module brief (docs/PLAN.md M11), kept deliberately separate from the mutable
 * {@link RefundSagaState} row: this one is written exactly once, at reservation time, and never
 * updated afterwards, regardless of how the saga later resolves. Whether the reservation is still
 * "active" is derived from {@code refund_saga_state.status == RESERVED}, not from a status column
 * on this table - see {@code adapters.out.persistence.RefundWriteTransaction}'s class javadoc for
 * why duplicating that status here was rejected (it is exactly the kind of second source of truth
 * that drifts).
 *
 * @param id         this reservation's own id - ledger-internal bookkeeping, never placed on the
 *                   wire (psp-connector and every downstream consumer correlate on
 *                   {@code refundId} alone).
 * @param refundId   the refund this reservation is for; unique per reservation.
 * @param paymentId  the payment being refunded.
 * @param merchantId the merchant whose balance was debited.
 * @param amount     the reserved (= requested) amount; refunds.* has no partial reservations.
 * @param reservedAt when the reservation was made - the TTL sweeper compares
 *                   {@code refund_saga_state.updated_at} against this same instant, since a
 *                   RESERVED row's {@code updated_at} is set at the same time this row is written.
 */
public record RefundReservation(
        UUID id, UUID refundId, UUID paymentId, String merchantId, Money amount, Instant reservedAt) {

    public RefundReservation {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(refundId, "refundId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        if (amount.amount().signum() <= 0) {
            throw new IllegalArgumentException(
                    "reservation amount must be strictly positive, was " + amount.amount());
        }
        Objects.requireNonNull(reservedAt, "reservedAt must not be null");
    }

    /** Creates a fresh reservation to attempt - {@code id} minted here, never reconstituted. */
    public static RefundReservation newReservation(
            UUID refundId, UUID paymentId, String merchantId, Money amount) {
        return new RefundReservation(UUID.randomUUID(), refundId, paymentId, merchantId, amount, Instant.now());
    }
}
