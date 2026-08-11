package com.example.psp.analytics.domain.model;

/**
 * The aggregate state held in the windowed state store, one instance per (merchantId, window)
 * pair (M10). Pure Java (ADR-0007).
 *
 * <p><b>Counters, not derived values.</b> The record stores {@code totalCount},
 * {@code declinedCount} and {@code latencySumMillis}; decline rate and average latency are
 * computed on read. That is not a style preference - a windowed aggregate is folded incrementally
 * over an unbounded number of records and is restored by <i>replaying stored values</i>, so the
 * state has to be composable ({@code plus} must be associative) and re-derivable. An average
 * cannot be updated from an average; a sum and a count can.
 *
 * <p><b>Why this type and not the Avro record.</b> Every value stored in a Kafka Streams state
 * store is also written to that store's changelog topic, so this class's serialized form is a
 * durable wire format even though it is never published to a business topic. It is serialized
 * with {@code JsonSerde} (see {@code adapters.in.kafka.AnalyticsTopology}), which keeps
 * {@code analytics-streams.v1-merchant-metrics-1m-changelog} human-readable in AKHQ - a real
 * convenience when the whole point of the module's proof is watching state restore from that
 * topic. The cost is honest: adding a field to this record changes the changelog format with no
 * Schema Registry to enforce compatibility, so it must be treated as a breaking change that
 * requires a Streams application reset.
 *
 * @param totalCount                   payment status events in this window for this merchant.
 * @param declinedCount                of which were {@code DECLINED}.
 * @param latencySumMillis             sum of {@link PaymentOutcome#pipelineLatencyMillis()}.
 * @param merchantDisplayName          last non-null display name seen from the joined config.
 * @param declineRateAlertThresholdBps last non-null threshold seen from the joined config, in
 *                                     basis points; {@code null} when no config has ever joined.
 */
public record MerchantWindowMetrics(
        long totalCount,
        long declinedCount,
        long latencySumMillis,
        String merchantDisplayName,
        Integer declineRateAlertThresholdBps) {

    private static final MerchantWindowMetrics EMPTY = new MerchantWindowMetrics(0L, 0L, 0L, null, null);

    /** The {@code Initializer} for the windowed aggregation - a fresh window starts here. */
    public static MerchantWindowMetrics empty() {
        return EMPTY;
    }

    /**
     * The {@code Aggregator}. Associative and commutative over the counter fields, which is what
     * makes it safe under at-least-once redelivery ordering and under restore.
     *
     * <p>The two enrichment fields are last-write-wins with a null guard: a merchant whose config
     * arrives (or is tombstoned) mid-window keeps the most recent name/threshold the window has
     * seen, rather than flapping to null and losing the label. A tombstone therefore stops
     * <i>new</i> windows from being labelled without corrupting the one in flight.
     */
    public MerchantWindowMetrics plus(PaymentOutcome outcome) {
        return new MerchantWindowMetrics(
                totalCount + 1,
                declinedCount + (outcome.declined() ? 1 : 0),
                latencySumMillis + outcome.pipelineLatencyMillis(),
                outcome.merchantDisplayName() != null ? outcome.merchantDisplayName() : merchantDisplayName,
                outcome.declineRateAlertThresholdBps() != null
                        ? outcome.declineRateAlertThresholdBps()
                        : declineRateAlertThresholdBps);
    }

    /** Declined / total, in [0, 1]. Zero for an empty window rather than NaN. */
    public double declineRate() {
        return totalCount == 0 ? 0.0d : (double) declinedCount / (double) totalCount;
    }

    /** Same, in basis points, for comparison against {@link #declineRateAlertThresholdBps()}. */
    public long declineRateBps() {
        return totalCount == 0 ? 0L : Math.round(10_000.0d * declinedCount / totalCount);
    }

    /** Mean {@link PaymentOutcome#pipelineLatencyMillis()} over the window. */
    public double avgPipelineLatencyMillis() {
        return totalCount == 0 ? 0.0d : (double) latencySumMillis / (double) totalCount;
    }

    /**
     * The one thing the {@code GlobalKTable} join is actually <i>for</i>: a per-merchant
     * threshold applied to a per-merchant window. False when no config has joined (an unknown
     * merchant cannot breach a threshold it does not have) and when the threshold is 0 (opted
     * out).
     */
    public boolean declineRateAlert() {
        return declineRateAlertThresholdBps != null
                && declineRateAlertThresholdBps > 0
                && totalCount > 0
                && declineRateBps() >= declineRateAlertThresholdBps;
    }
}
