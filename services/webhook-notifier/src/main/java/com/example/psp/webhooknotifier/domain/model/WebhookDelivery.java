package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;
import java.util.UUID;

public record WebhookDelivery(
        UUID id,
        String eventType,
        UUID paymentId,
        UUID refundId,
        String merchantId,
        String status,
        int attempts,
        Instant lastAttemptAt,
        Instant createdAt) {}
