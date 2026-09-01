package com.example.psp.webhooknotifier.domain.model;

import java.math.BigDecimal;
import java.util.UUID;

public record WebhookDeliveryCommand(
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
