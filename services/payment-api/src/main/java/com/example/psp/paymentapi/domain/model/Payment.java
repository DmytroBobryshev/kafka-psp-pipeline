package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public final class Payment {

    @EqualsAndHashCode.Include
    private final UUID id;

    private final String merchantId;
    private final Money amount;
    private final Instant createdAt;
    private PaymentStatus status;

    private final Instant statusUpdatedAt;

    private Payment(
            UUID id,
            String merchantId,
            Money amount,
            PaymentStatus status,
            Instant createdAt,
            Instant statusUpdatedAt) {
        this.statusUpdatedAt = statusUpdatedAt;
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.merchantId = requireNonBlank(merchantId, "merchantId");
        this.amount = Objects.requireNonNull(amount, "amount must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    }

    public static Payment create(String merchantId, Money amount) {
        return new Payment(UUID.randomUUID(), merchantId, amount, PaymentStatus.CREATED, Instant.now(), null);
    }

    public static Payment reconstitute(
            UUID id,
            String merchantId,
            Money amount,
            PaymentStatus status,
            Instant createdAt,
            Instant statusUpdatedAt) {
        return new Payment(id, merchantId, amount, status, createdAt, statusUpdatedAt);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
