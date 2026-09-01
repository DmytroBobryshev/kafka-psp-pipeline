package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.List;

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
        // M24: exact field name is load-bearing - the UI reads "refundExpirationSeconds" verbatim.
        int refundExpirationSeconds,
        Instant updatedAt) {
}
