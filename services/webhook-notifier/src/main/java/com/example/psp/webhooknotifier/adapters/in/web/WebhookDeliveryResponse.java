package com.example.psp.webhooknotifier.adapters.in.web;

import java.time.Instant;

public record WebhookDeliveryResponse(
        String id,
        String eventType,
        String paymentId,
        String refundId,
        String merchantId,
        String url,
        String status,
        int attempts,
        Instant lastAttemptAt,
        Instant createdAt) {}
