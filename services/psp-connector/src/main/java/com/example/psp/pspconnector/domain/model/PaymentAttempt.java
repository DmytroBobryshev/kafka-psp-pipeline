package com.example.psp.pspconnector.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * One record of psp-connector attempting to authorize a payment against the (simulated) provider.
 * Pure Java, no framework dependency (ADR-0007) - Lombok is allowed in {@code domain/} because it
 * is compile-time-only (see {@code payment-api}'s {@code Payment} javadoc for the same note).
 *
 * <p>This is the M4 shape of the dedup table row described in the module brief: keyed
 * {@code (paymentId, providerEventId)} with a unique constraint (see
 * {@code db/migration/V1__create_payment_attempts_table.sql}). M4 only ever <b>inserts</b> a row
 * here - "just record attempts" - it never reads this table back to decide whether to skip a
 * re-authorization. That check (query-before-call, making a redelivered
 * {@code payments.payment-requested.v1} record a no-op instead of a second provider charge) is
 * M5's "real idempotency" work.
 *
 * <p>{@code causationEventId}/{@code traceId}/{@code correlationId} are carried on the attempt
 * itself, not passed around as a separate parameter, so that (a) the persisted row is a useful
 * audit/debugging trail on its own, matching the inbound event that caused it, and (b)
 * {@code domain.port.PaymentStatusPublisher} can build a causally-chained {@code EventEnvelope}
 * from the attempt alone, without depending on the {@code application/} command type (a port
 * signature may only reference {@code domain/} types - ADR-0007).
 */
@Getter
public final class PaymentAttempt {

    private final UUID id;
    private final UUID paymentId;
    private final String merchantId;
    private final Money amount;
    private final UUID providerEventId;
    private final ProviderOutcome outcome;
    private final long providerLatencyMs;
    private final UUID causationEventId;
    // The envelope eventId the outbound status event carries - minted once, when the attempt is
    // first processed, and persisted with the row, so a republish after redelivery reuses the
    // SAME downstream idempotency key (M19 drill 9). Nullable only for rows persisted before
    // db/migration/V4 added the column.
    private final UUID statusEventId;
    private final String traceId;
    private final String correlationId;
    private final Instant processedAt;

    private PaymentAttempt(
            UUID id,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerEventId,
            ProviderOutcome outcome,
            long providerLatencyMs,
            UUID causationEventId,
            UUID statusEventId,
            String traceId,
            String correlationId,
            Instant processedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.merchantId = requireNonBlank(merchantId, "merchantId");
        this.amount = Objects.requireNonNull(amount, "amount must not be null");
        this.providerEventId = Objects.requireNonNull(providerEventId, "providerEventId must not be null");
        this.outcome = Objects.requireNonNull(outcome, "outcome must not be null");
        this.providerLatencyMs = providerLatencyMs;
        this.causationEventId = Objects.requireNonNull(causationEventId, "causationEventId must not be null");
        // Deliberately NOT requireNonNull: pre-V4 rows reconstitute with a null statusEventId.
        this.statusEventId = statusEventId;
        this.traceId = requireNonBlank(traceId, "traceId");
        this.correlationId = requireNonBlank(correlationId, "correlationId");
        this.processedAt = Objects.requireNonNull(processedAt, "processedAt must not be null");
    }

    /**
     * Creates a freshly-processed attempt (M4: always a new one, never reconstituted from
     * storage). {@code statusEventId} is minted by the caller (the application layer, which owns
     * the UUIDv7 convention - ADR-0002) rather than here, because {@code domain/} imports nothing
     * outside itself (ADR-0007) and {@code UuidV7} lives in {@code common-events}.
     */
    public static PaymentAttempt from(
            UUID paymentId,
            String merchantId,
            Money amount,
            ProviderResult result,
            UUID causationEventId,
            UUID statusEventId,
            String traceId,
            String correlationId) {
        Objects.requireNonNull(result, "result must not be null");
        Objects.requireNonNull(statusEventId, "statusEventId must not be null");
        return new PaymentAttempt(
                UUID.randomUUID(),
                paymentId,
                merchantId,
                amount,
                result.providerEventId(),
                result.outcome(),
                result.latencyMs(),
                causationEventId,
                statusEventId,
                traceId,
                correlationId,
                Instant.now());
    }

    /**
     * Reconstitutes an attempt from persisted state (M12) - the counterpart to {@link #from},
     * which only ever builds a freshly-processed attempt. Needed because
     * {@code domain.port.AttemptLogRepository#findLatestByPaymentId} (M12's read path, the first
     * one this table has ever had - see that method's javadoc) must hand back a real
     * {@link PaymentAttempt}, and this class has no public constructor or setters for MapStruct's
     * default bean-mapping strategy to target (same reasoning as {@code payment-api}'s
     * {@code Payment#reconstitute}).
     */
    public static PaymentAttempt reconstitute(
            UUID id,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerEventId,
            ProviderOutcome outcome,
            long providerLatencyMs,
            UUID causationEventId,
            UUID statusEventId,
            String traceId,
            String correlationId,
            Instant processedAt) {
        return new PaymentAttempt(
                id,
                paymentId,
                merchantId,
                amount,
                providerEventId,
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
