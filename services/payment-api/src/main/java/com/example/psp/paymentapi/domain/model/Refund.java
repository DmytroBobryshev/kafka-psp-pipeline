package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * The {@code Refund} aggregate root (M11) - payment-api's own local record of a refund it
 * requested, entirely separate from the ledger's {@code refund_saga_state} (ADR-0008 rule 1: each
 * saga participant persists its own view; there is no shared saga table). Pure Java, no framework
 * dependency (ADR-0007) - same identity-based-equality convention as {@link Payment}.
 *
 * <p>This aggregate's status is always {@code REQUESTED} and never advances: payment-api does not
 * consume any downstream refunds.* event in this module's declared scope (that state machine lives
 * in the ledger - see services/ledger/README.md's M11 section). This row exists so
 * {@code POST /payments/{paymentId}/refunds} can (a) validate a new request against previously
 * requested amounts for the same payment, and (b) be the transactional-outbox partner that makes
 * "record the refund" and "publish refunds.refund-requested.v1" atomic - the exact M6 pattern
 * {@link Payment}/{@code CreatePaymentUseCase} already established, applied to a second aggregate.
 */
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

    /** Creates a brand-new refund request. */
    public static Refund request(UUID paymentId, String merchantId, Money amount, String reason) {
        return new Refund(UUID.randomUUID(), paymentId, merchantId, amount, reason, Instant.now());
    }

    /** Reconstitutes a refund from persisted state - used by {@code adapters/out/persistence}. */
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
