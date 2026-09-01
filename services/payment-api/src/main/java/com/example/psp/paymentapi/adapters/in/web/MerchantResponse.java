package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.List;

/**
 * Wire contract for a single merchant, returned by both {@code GET /api/merchants} (as an
 * {@code items} entry) and {@code GET /api/merchants/{id}}. Field names are load-bearing - the UI
 * is built directly against this exact shape.
 */
public record MerchantResponse(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        // M22: exact field name is load-bearing - the UI reads "paymentExpirationSeconds" verbatim.
        int paymentExpirationSeconds,
        Instant updatedAt) {
}
