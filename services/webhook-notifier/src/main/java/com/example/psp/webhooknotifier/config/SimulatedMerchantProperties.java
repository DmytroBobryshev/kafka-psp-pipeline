package com.example.psp.webhooknotifier.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "webhook-notifier.simulated-merchant")
public record SimulatedMerchantProperties(
        ForcedOutcome forcedOutcome,
        double serverErrorRate,
        double clientErrorRate,
        double timeoutRate,
        long latencyMs,
        long timeoutDelayMs) {

    public SimulatedMerchantProperties {
        if (forcedOutcome == null) {
            forcedOutcome = ForcedOutcome.NONE;
        }
        double total = serverErrorRate + clientErrorRate + timeoutRate;
        if (serverErrorRate < 0 || clientErrorRate < 0 || timeoutRate < 0 || total > 1.0) {
            throw new IllegalArgumentException(
                    "serverErrorRate + clientErrorRate + timeoutRate must be within [0,1], got " + total);
        }
    }

    public enum ForcedOutcome {
        NONE,
        SUCCESS,
        CLIENT_ERROR,
        SERVER_ERROR,
        TIMEOUT
    }
}
