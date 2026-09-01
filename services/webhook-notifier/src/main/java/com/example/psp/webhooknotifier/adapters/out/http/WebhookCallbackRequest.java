package com.example.psp.webhooknotifier.adapters.out.http;

import java.math.BigDecimal;

public record WebhookCallbackRequest(
        String paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        String eventType,
        String refundId) {}
