package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * The {@code Payment} aggregate root. Pure Java, no framework dependency (ADR-0007) - this is
 * not a JPA entity, so the "no {@code @Data} on JPA entities" rule (PLAN.md) doesn't literally
 * apply, but the same reasoning does: identity-based equality, not value equality, is what an
 * aggregate needs, so equality is scoped to {@code id} rather than generated over every field.
 *
 * <p>Lombok is allowed in {@code domain/} - it is a compile-time-only annotation processor and
 * leaves no runtime dependency on any framework, so it doesn't violate the "domain compiles with
 * zero framework dependencies" test (enforced by the ArchUnit rule in this service).
 */
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public final class Payment {

    @EqualsAndHashCode.Include
    private final UUID id;

    private final String merchantId;
    private final Money amount;
    private final Instant createdAt;
    private PaymentStatus status;

    // When the outcome landed (the status listener's UPDATE stamps it); null while CREATED and
    // for rows that resolved before db/migration/V6 added the column.
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

    /** Creates a brand-new payment in {@link PaymentStatus#CREATED}. */
    public static Payment create(String merchantId, Money amount) {
        return new Payment(UUID.randomUUID(), merchantId, amount, PaymentStatus.CREATED, Instant.now(), null);
    }

    /** Reconstitutes a payment from persisted state - used by {@code adapters/out/persistence}. */
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
