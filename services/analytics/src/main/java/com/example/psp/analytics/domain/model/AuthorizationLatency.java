package com.example.psp.analytics.domain.model;

import java.time.Duration;
import java.time.Instant;

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
