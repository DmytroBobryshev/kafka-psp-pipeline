package com.example.psp.webhooknotifier.adapters.out.kafka;

import java.math.BigDecimal;
import java.util.UUID;

public record WebhookDeliveryRequested(
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        UUID causationEventId,
        String traceId,
        String correlationId,
        String eventType,
        UUID refundId) {}
