package com.example.psp.pspconnector.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * One record of psp-connector executing a refund against the (simulated) provider (M11) - the
 * refund-path counterpart of {@link PaymentAttempt}. Pure Java, no framework dependency
 * (ADR-0007).
 *
 * <p>Unlike {@code PaymentAttempt}, this attempt carries only ONE idempotency concern: the inbound
 * {@code refunds.funds-reserved.v1} event's own {@code eventId} ({@code causationEventId}) -
 * M5 level 1, replay/consumer idempotency. This module deliberately does not replicate M5's level
 * 2 (duplicate provider callback, keyed on a provider-minted id) for the refund path - see
 * services/psp-connector/README.md's M11 section for why that simplification was accepted.
 */
@Getter
public final class RefundAttempt {

    private final UUID id;
    private final UUID refundId;
    private final UUID paymentId;
    private final String merchantId;
    private final Money amount;
    private final UUID providerReference;
    private final RefundOutcome outcome;
    private final long providerLatencyMs;
    private final UUID causationEventId;
    // Same contract as PaymentAttempt#statusEventId: the outbound event's envelope eventId,
    // minted once and persisted so a redelivery republishes the SAME logical event (M19 drill 9).
    // Nullable only for rows persisted before db/migration/V4 added the column.
    private final UUID statusEventId;
    private final String traceId;
    private final String correlationId;
    private final Instant processedAt;

    private RefundAttempt(
            UUID id,
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            RefundOutcome outcome,
            long providerLatencyMs,
            UUID causationEventId,
            UUID statusEventId,
            String traceId,
            String correlationId,
            Instant processedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.refundId = Objects.requireNonNull(refundId, "refundId must not be null");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.merchantId = requireNonBlank(merchantId, "merchantId");
        this.amount = Objects.requireNonNull(amount, "amount must not be null");
        this.providerReference = Objects.requireNonNull(providerReference, "providerReference must not be null");
        this.outcome = Objects.requireNonNull(outcome, "outcome must not be null");
        this.providerLatencyMs = providerLatencyMs;
        this.causationEventId = Objects.requireNonNull(causationEventId, "causationEventId must not be null");
        // Deliberately NOT requireNonNull: pre-V4 rows reconstitute with a null statusEventId.
        this.statusEventId = statusEventId;
        this.traceId = requireNonBlank(traceId, "traceId");
        this.correlationId = requireNonBlank(correlationId, "correlationId");
        this.processedAt = Objects.requireNonNull(processedAt, "processedAt must not be null");
    }

    /** See {@link PaymentAttempt#from} for why {@code statusEventId} is minted by the caller. */
    public static RefundAttempt from(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            RefundProviderResult result,
            UUID causationEventId,
            UUID statusEventId,
            String traceId,
            String correlationId) {
        Objects.requireNonNull(result, "result must not be null");
        Objects.requireNonNull(statusEventId, "statusEventId must not be null");
        return new RefundAttempt(
                UUID.randomUUID(),
                refundId,
                paymentId,
                merchantId,
                amount,
                result.providerReference(),
                result.outcome(),
                result.latencyMs(),
                causationEventId,
                statusEventId,
                traceId,
                correlationId,
                Instant.now());
    }

    /**
     * Reconstitutes an attempt from persisted state - needed since the M19 drill 9 fix gave this
     * table its first read path ({@code RefundAttemptLogRepository#findByInboundEventId}, the
     * republish-on-redelivery lookup). Counterpart of {@link PaymentAttempt#reconstitute}.
     */
    public static RefundAttempt reconstitute(
            UUID id,
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            RefundOutcome outcome,
            long providerLatencyMs,
            UUID causationEventId,
            UUID statusEventId,
            String traceId,
            String correlationId,
            Instant processedAt) {
        return new RefundAttempt(
                id,
                refundId,
                paymentId,
                merchantId,
                amount,
                providerReference,
                outcome,
                providerLatencyMs,
                causationEventId,
                statusEventId,
                traceId,
                correlationId,
                processedAt);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
