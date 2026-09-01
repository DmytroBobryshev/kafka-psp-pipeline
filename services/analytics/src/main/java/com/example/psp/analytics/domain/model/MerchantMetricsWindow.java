package com.example.psp.analytics.domain.model;

import java.time.Instant;
import java.util.Objects;

public record MerchantMetricsWindow(
        String merchantId, Instant windowStart, Instant windowEnd, MerchantWindowMetrics metrics) {

    public MerchantMetricsWindow {
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(windowStart, "windowStart must not be null");
        Objects.requireNonNull(windowEnd, "windowEnd must not be null");
        Objects.requireNonNull(metrics, "metrics must not be null");
        if (!windowEnd.isAfter(windowStart)) {
            throw new IllegalArgumentException("windowEnd must be after windowStart");
        }
    }

    public String key() {
        return merchantId + "|" + windowStart.toEpochMilli();
    }
}
