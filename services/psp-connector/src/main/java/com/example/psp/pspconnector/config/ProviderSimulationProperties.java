package com.example.psp.pspconnector.config;

import com.example.psp.pspconnector.adapters.out.http.SimulatedPaymentProviderAdapter.ForcedOutcome;
import com.example.psp.pspconnector.adapters.out.http.SimulatedPaymentProviderAdapter.RefundForcedOutcome;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "psp-connector.provider")
public record ProviderSimulationProperties(
        long minLatencyMs,
        long maxLatencyMs,
        double declineRate,
        double timeoutRate,
        long forcedLatencyMs,
        ForcedOutcome forcedOutcome,
        double duplicateRate,
        double refundDeclineRate,
        RefundForcedOutcome refundForcedOutcome,
        MagicAmounts magicAmounts) {

    public ProviderSimulationProperties {
        if (minLatencyMs < 0 || maxLatencyMs < minLatencyMs) {
            throw new IllegalArgumentException(
                    "minLatencyMs must be >= 0 and <= maxLatencyMs, got min="
                            + minLatencyMs
                            + " max="
                            + maxLatencyMs);
        }
        if (declineRate < 0 || timeoutRate < 0 || declineRate + timeoutRate > 1.0) {
            throw new IllegalArgumentException(
                    "declineRate + timeoutRate must be within [0,1], got decline="
                            + declineRate
                            + " timeout="
                            + timeoutRate);
        }
        if (duplicateRate < 0 || duplicateRate > 1.0) {
            throw new IllegalArgumentException(
                    "duplicateRate must be within [0,1], got " + duplicateRate);
        }
        if (refundDeclineRate < 0 || refundDeclineRate > 1.0) {
            throw new IllegalArgumentException(
                    "refundDeclineRate must be within [0,1], got " + refundDeclineRate);
        }
        if (forcedOutcome == null) {
            forcedOutcome = ForcedOutcome.NONE;
        }
        if (refundForcedOutcome == null) {
            refundForcedOutcome = RefundForcedOutcome.NONE;
        }
        if (magicAmounts == null) {
            magicAmounts = new MagicAmounts(true);
        }
    }

    public record MagicAmounts(boolean enabled) {}
}
