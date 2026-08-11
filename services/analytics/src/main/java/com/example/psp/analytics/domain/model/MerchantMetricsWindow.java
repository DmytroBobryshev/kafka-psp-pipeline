package com.example.psp.analytics.domain.model;

import java.time.Instant;
import java.util.Objects;

/**
 * A closed-over view of one (merchant, window) result: the window bounds plus the metrics that
 * fell inside them (M10). Pure Java (ADR-0007).
 *
 * <p>Kept separate from {@link MerchantWindowMetrics} on purpose. The aggregate stored in the
 * state store must be window-agnostic, because Kafka Streams keeps the window in the <b>key</b>
 * ({@code Windowed<String>}), not in the value - duplicating {@code windowStart} into the value
 * would be a second, silently-divergable copy of the same fact. This type is where the two are
 * recombined, at the {@code toStream()} boundary, for the projection and the REST response.
 */
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

    /**
     * Stable identity of this result: one document per merchant per window. Used as the MongoDB
     * {@code _id}, which is what makes the projection idempotent under at-least-once processing -
     * a re-emitted window overwrites its own document instead of appending a duplicate.
     */
    public String key() {
        return merchantId + "|" + windowStart.toEpochMilli();
    }
}
