package com.example.psp.pspconnector.domain.model;

import java.util.Objects;
import java.util.UUID;

/**
 * The outcome of one call to {@code domain.port.PaymentProviderPort}. {@code providerEventId} is
 * an id minted by the (simulated) provider for this specific attempt/callback - not our own
 * {@code EventEnvelope.eventId} - because a real acquirer assigns its own event/transaction ids
 * independently of anything we generate. It is what the M4 dedup table is keyed on alongside
 * {@code paymentId}, and what M5 turns into real idempotency.
 *
 * @param providerEventId the provider's id for this attempt, always present even on a timeout -
 *                         a real provider that never responded may still have logged the attempt
 *                         on its side; we generate a placeholder id for the same reason we still
 *                         write an attempt row (see {@code application.ProcessPaymentRequestUseCase}).
 * @param outcome          {@link ProviderOutcome#APPROVED}, {@link ProviderOutcome#DECLINED}, or
 *                         {@link ProviderOutcome#TIMEOUT}.
 * @param latencyMs        simulated round-trip time actually spent, for observability.
 */
public record ProviderResult(UUID providerEventId, ProviderOutcome outcome, long latencyMs) {

    public ProviderResult {
        Objects.requireNonNull(providerEventId, "providerEventId must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (latencyMs < 0) {
            throw new IllegalArgumentException("latencyMs must not be negative, was " + latencyMs);
        }
    }
}
