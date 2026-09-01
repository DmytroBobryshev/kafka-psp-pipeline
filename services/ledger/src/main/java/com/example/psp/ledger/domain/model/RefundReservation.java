package com.example.psp.ledger.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

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

    public static RefundReservation newReservation(
            UUID refundId, UUID paymentId, String merchantId, Money amount) {
        return new RefundReservation(UUID.randomUUID(), refundId, paymentId, merchantId, amount, Instant.now());
    }
}
