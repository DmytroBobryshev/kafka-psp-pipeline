package com.example.psp.pspconnector.domain.model;

import java.util.Objects;
import java.util.UUID;

/**
 * The outcome of one call to {@code domain.port.RefundProviderPort} (M11) - the refund-path
 * counterpart of {@link ProviderResult}. {@code providerReference} is minted by the (simulated)
 * provider for this specific refund attempt, independent of anything this service generates.
 *
 * @param providerReference the provider's reference for this refund attempt (always present, even
 *                           on a decline - the provider still logged the attempt on its side).
 * @param outcome            {@link RefundOutcome#COMPLETED} or {@link RefundOutcome#DECLINED}.
 * @param latencyMs          simulated round-trip time actually spent, for observability.
 */
public record RefundProviderResult(UUID providerReference, RefundOutcome outcome, long latencyMs) {

    public RefundProviderResult {
        Objects.requireNonNull(providerReference, "providerReference must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (latencyMs < 0) {
            throw new IllegalArgumentException("latencyMs must not be negative, was " + latencyMs);
        }
    }
}
