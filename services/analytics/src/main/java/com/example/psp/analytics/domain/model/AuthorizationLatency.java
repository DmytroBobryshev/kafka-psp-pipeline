package com.example.psp.analytics.domain.model;

import java.time.Duration;
import java.time.Instant;

/**
 * One payment's <b>authorization latency</b>: the time between the payment being requested and
 * its status being decided (M13) - the real measure services/analytics/README.md's M10 section
 * explicitly says {@code avgPipelineLatencyMillis} is NOT.
 *
 * <p>Pure Java (ADR-0007): no Kafka, no Avro. Produced by the M13 stream-stream join's
 * {@code ValueJoiner} in {@code adapters.in.kafka.AnalyticsTopology}, from
 * {@code payments.payment-requested.v1} (paymentId-keyed) and
 * {@code payments.payment-status-changed.v1} (merchantId-keyed, re-keyed to paymentId to make
 * the join possible at all - see the topology's javadoc for why that repartition is required and
 * a {@code GlobalKTable} cannot substitute for it).
 *
 * @param paymentId         the join key - present, unmodified, on both source records.
 * @param merchantId        from the status-changed side; the payment-requested side carries the
 *                          same value but is not the record this join is keyed by.
 * @param providerReference the (simulated) provider's event id for the decision.
 * @param status            {@code SUCCEEDED} or {@code DECLINED}.
 * @param declined          {@code true} when {@code status} is {@code DECLINED}.
 * @param requestedAt       {@code payments.payment-requested.v1}'s {@code envelope.occurredAt}.
 * @param decidedAt         {@code payments.payment-status-changed.v1}'s {@code envelope.occurredAt}.
 * @param latencyMillis     {@code decidedAt - requestedAt}, clamped to zero. This is a genuine
 *                          authorization latency, unlike M10's {@code avgPipelineLatencyMillis}
 *                          (which measures provider-answered-to-analytics-processed, not
 *                          requested-to-decided).
 */
public record AuthorizationLatency(
        String paymentId,
        String merchantId,
        String providerReference,
        String status,
        boolean declined,
        Instant requestedAt,
        Instant decidedAt,
        long latencyMillis) {

    private static final String DECLINED = "DECLINED";

    /**
     * The join's {@code ValueJoiner} calls this - see the extensive javadoc on it. The clamp to
     * zero is defensive, not load-bearing: the join window itself (see the topology) already
     * excludes a {@code decidedAt} timestamped earlier than {@code requestedAt} by construction
     * ({@code JoinWindows.before(Duration.ZERO)}), but that window is enforced on Kafka Streams'
     * own record timestamps (from {@code EnvelopeEventTimeExtractor}), while this arithmetic
     * re-reads {@code envelope.occurredAt} directly from each Avro record - the same two values,
     * read the same way, but a defensive clamp costs nothing and matches
     * {@code AnalyticsTopology#toOutcome}'s identical guard for the M10 latency figure.
     */
    public static AuthorizationLatency of(
            String paymentId,
            String merchantId,
            String providerReference,
            String status,
            Instant requestedAt,
            Instant decidedAt) {
        long latencyMillis = Math.max(0L, Duration.between(requestedAt, decidedAt).toMillis());
        return new AuthorizationLatency(
                paymentId,
                merchantId,
                providerReference,
                status,
                DECLINED.equalsIgnoreCase(status),
                requestedAt,
                decidedAt,
                latencyMillis);
    }
}
