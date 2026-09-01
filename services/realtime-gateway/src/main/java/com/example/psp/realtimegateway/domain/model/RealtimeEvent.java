package com.example.psp.realtimegateway.domain.model;

import java.time.Instant;

public record RealtimeEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String causationId,
        String source,
        String paymentId,
        String merchantId,
        String refundId,
        String status,
        String reason,
        String providerReference) {
}
