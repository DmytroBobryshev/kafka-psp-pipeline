package com.example.psp.pspconnector.config;

import com.example.psp.pspconnector.adapters.out.http.SimulatedPaymentProviderAdapter.ForcedOutcome;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code psp-connector.provider.*} from {@code application.yml}. Every field here exists so
 * an experiment can override it from the command line without a code change - see the README's
 * "Prove it" section for the exact overrides used for the rebalance-storm and duplicates-vs-loss
 * drills (e.g. {@code --psp-connector.provider.forced-latency-ms=10000}).
 *
 * @param minLatencyMs    lower bound of the simulated provider round-trip, inclusive.
 * @param maxLatencyMs    upper bound of the simulated provider round-trip, inclusive.
 * @param declineRate     probability (0.0-1.0) of a {@code DECLINED} outcome.
 * @param timeoutRate     probability (0.0-1.0) of a {@code TIMEOUT} outcome. {@code declineRate +
 *                        timeoutRate} MUST be &lt;= 1.0; the remainder is the approval rate.
 * @param forcedLatencyMs when &gt; 0, every call takes exactly this long instead of a random
 *                        value in {@code [minLatencyMs, maxLatencyMs]}. 0 disables the override.
 * @param forcedOutcome    when not {@code NONE}, every call resolves to this outcome instead of
 *                        rolling against {@code declineRate}/{@code timeoutRate}.
 * @param duplicateRate    probability (0.0-1.0) that a repeat {@code authorize()} call for a
 *                        {@code paymentId} this adapter instance has already seen replays the
 *                        identical previous {@link com.example.psp.pspconnector.domain.model.ProviderResult}
 *                        (same {@code providerEventId}) instead of minting a fresh one - M5's
 *                        "deliberate duplicate emission" knob (docs/PLAN.md), simulating a
 *                        provider that redelivers/replays its own callback for the same logical
 *                        attempt rather than a client-side retry legitimately reaching the
 *                        provider a second time. Default 0 preserves M4 behaviour: every call
 *                        gets a brand-new {@code providerEventId}, even for a paymentId seen
 *                        before.
 */
@ConfigurationProperties(prefix = "psp-connector.provider")
public record ProviderSimulationProperties(
        long minLatencyMs,
        long maxLatencyMs,
        double declineRate,
        double timeoutRate,
        long forcedLatencyMs,
        ForcedOutcome forcedOutcome,
        double duplicateRate) {

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
        if (forcedOutcome == null) {
            forcedOutcome = ForcedOutcome.NONE;
        }
    }
}
