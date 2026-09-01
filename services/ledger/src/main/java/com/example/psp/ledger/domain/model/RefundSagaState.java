package com.example.psp.ledger.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

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
