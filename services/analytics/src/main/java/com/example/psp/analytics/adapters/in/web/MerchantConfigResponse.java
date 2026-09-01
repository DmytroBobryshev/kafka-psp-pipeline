package com.example.psp.analytics.adapters.in.web;

public record MerchantConfigResponse(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps) {
}
