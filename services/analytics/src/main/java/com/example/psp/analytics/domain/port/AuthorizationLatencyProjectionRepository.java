package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.AuthorizationLatency;

/**
 * Outbound port for the M13 authorization-latency projection.
 *
 * <p>Same shape and same idempotency reasoning as {@link MetricsProjectionRepository} (M10): the
 * write is outside Kafka's guarantee (services/ledger/README.md's "Where Kafka EOS ends"), so it
 * has to be idempotent on its own rather than rely on {@code processing.guarantee}. Here the
 * natural idempotency key is even simpler than M10's {@code merchantId|windowStart} composite:
 * {@code paymentId} alone, because a payment has exactly one authorization decision - the join
 * that produces {@link AuthorizationLatency} emits at most once per {@code paymentId} in the
 * steady state, and at-least-once redelivery of the same join result is a same-document replace,
 * not a duplicate.
 */
public interface AuthorizationLatencyProjectionRepository {

    /** Idempotent upsert of one payment's authorization latency, keyed by {@code paymentId}. */
    void save(AuthorizationLatency latency);
}
