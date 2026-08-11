package com.example.psp.webhooknotifier.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code webhook-notifier.simulated-merchant.*} - the knobs behind
 * {@code adapters.in.web.SimulatedMerchantController}, M8's in-process stand-in for a real
 * merchant endpoint. Every field exists so an experiment can force a specific outcome without a
 * code change - the same role {@code psp-connector.provider.*} / {@code ProviderSimulationProperties}
 * plays for M4's simulated provider.
 *
 * <p>Two independent ways to force an outcome, both documented in the README:
 *
 * <ul>
 *   <li><b>Global</b> - {@link #forcedOutcome()} overrides every request, regardless of merchant.
 *   <li><b>Per-merchant</b> - a {@code merchantId} containing one of the literal substrings
 *       {@code force-success}/{@code force-4xx}/{@code force-5xx}/{@code force-timeout}
 *       deterministically forces that outcome for THAT merchant only, letting one experiment run
 *       mix outcomes (e.g. one payment for {@code merchant-force-4xx-1} and another for
 *       {@code merchant-ok-1} in the same batch) without touching config between requests. Checked
 *       before {@link #forcedOutcome()}... no - checked AFTER: see the controller for the actual
 *       precedence (global forced-outcome wins if set, since it is the more explicit, less
 *       ambiguous signal for a single-scenario experiment run).
 * </ul>
 *
 * @param forcedOutcome     {@code NONE} (use the rates below) or an override applied to every
 *                          request regardless of merchant id.
 * @param serverErrorRate   probability (0.0-1.0) of a 5xx response (ADR-0006 category A, retryable).
 * @param clientErrorRate   probability (0.0-1.0) of a 4xx response (non-retryable).
 * @param timeoutRate       probability (0.0-1.0) of deliberately exceeding the caller's read
 *                          timeout (retryable).
 * @param latencyMs         base processing delay applied to every request, success or failure -
 *                          simulates real merchant-side work. Safe to sleep on: this is an HTTP
 *                          request-handling thread, not a Kafka consumer poll thread, so it is
 *                          not subject to {@code max.poll.interval.ms} at all (contrast
 *                          {@code adapters.in.kafka.WebhookDeliveryExecutorListener}, which must
 *                          never sleep).
 * @param timeoutDelayMs    additional delay applied ONLY for a {@code TIMEOUT} outcome - MUST
 *                          exceed {@code webhook-notifier.merchant-client.read-timeout-ms} or the
 *                          caller will see a slow success instead of a timeout.
 */
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

    /** {@code webhook-notifier.simulated-merchant.forced-outcome} vocabulary. */
    public enum ForcedOutcome {
        NONE,
        SUCCESS,
        CLIENT_ERROR,
        SERVER_ERROR,
        TIMEOUT
    }
}
