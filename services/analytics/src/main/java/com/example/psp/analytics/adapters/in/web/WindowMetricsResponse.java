package com.example.psp.analytics.adapters.in.web;

import java.time.Instant;

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
