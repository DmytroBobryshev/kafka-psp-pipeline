package com.example.psp.pspconnector.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

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
