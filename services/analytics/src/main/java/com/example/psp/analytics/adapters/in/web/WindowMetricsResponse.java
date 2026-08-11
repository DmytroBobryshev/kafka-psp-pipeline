package com.example.psp.analytics.adapters.in.web;

import java.time.Instant;

/**
 * Wire contract for one (merchant, 1-minute window) result (M10). Records for DTOs, per PLAN.md.
 *
 * <p>Carries both the raw counters and the derived rates. The counters are what the state store
 * actually holds; the rates are computed by the domain record's accessors, so the REST response
 * and the MongoDB projection can never disagree about what "decline rate" means.
 *
 * @param avgPipelineLatencyMillis mean time from {@code envelope.occurredAt} (the provider
 *                                answered) to the moment analytics processed the event. Named for
 *                                what it is: this is pipeline latency, NOT payment authorization
 *                                latency - see
 *                                {@code domain.model.PaymentOutcome#pipelineLatencyMillis()}.
 * @param open                    {@code true} when wall-clock time is still inside this window,
 *                                i.e. the numbers are expected to keep moving. The single most
 *                                useful field for a human reading an interactive query, and one
 *                                the MongoDB projection cannot provide.
 */
public record WindowMetricsResponse(
        String merchantId,
        String merchantDisplayName,
        Instant windowStart,
        Instant windowEnd,
        boolean open,
        long totalCount,
        long declinedCount,
        double declineRate,
        long declineRateBps,
        double avgPipelineLatencyMillis,
        Integer declineRateAlertThresholdBps,
        boolean declineRateAlert) {
}
