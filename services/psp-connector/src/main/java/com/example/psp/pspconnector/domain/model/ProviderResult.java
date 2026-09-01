package com.example.psp.pspconnector.domain.model;

import java.util.Objects;
import java.util.UUID;

public record ProviderResult(UUID providerEventId, ProviderOutcome outcome, long latencyMs) {

    public ProviderResult {
        Objects.requireNonNull(providerEventId, "providerEventId must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (latencyMs < 0) {
            throw new IllegalArgumentException("latencyMs must not be negative, was " + latencyMs);
        }
    }
}
