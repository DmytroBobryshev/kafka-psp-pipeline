package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;
import java.util.UUID;

public record DeliveryAttempt(
        String merchantId,
        UUID paymentId,
        UUID refundId,
        String eventType,
        UUID causationEventId,
        int attemptNumber,
        DeliveryOutcome outcome,
        Integer statusCode,
        String error,
        String sourceTopic,
        Instant attemptedAt) {}
