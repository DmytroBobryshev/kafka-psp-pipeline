package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

@Getter
public final class Refund {

    private final UUID id;
    private final UUID paymentId;
    private final String merchantId;
    private final Money amount;
    private final String reason;
    private final Instant createdAt;

    private Refund(UUID id, UUID paymentId, String merchantId, Money amount, String reason, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.merchantId = requireNonBlank(merchantId, "merchantId");
        this.amount = Objects.requireNonNull(amount, "amount must not be null");
        if (amount.amount().signum() <= 0) {
            throw new IllegalArgumentException(
                    "refund amount must be strictly positive, was " + amount.amount());
        }
        this.reason = reason;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    }

    public static Refund request(UUID paymentId, String merchantId, Money amount, String reason) {
        return new Refund(UUID.randomUUID(), paymentId, merchantId, amount, reason, Instant.now());
    }

    public static Refund reconstitute(
            UUID id, UUID paymentId, String merchantId, Money amount, String reason, Instant createdAt) {
        return new Refund(id, paymentId, merchantId, amount, reason, createdAt);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
