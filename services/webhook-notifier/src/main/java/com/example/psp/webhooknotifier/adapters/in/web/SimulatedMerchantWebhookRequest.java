package com.example.psp.webhooknotifier.adapters.in.web;

import java.math.BigDecimal;

public record SimulatedMerchantWebhookRequest(
        String paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        String eventType,
        String refundId) {}
