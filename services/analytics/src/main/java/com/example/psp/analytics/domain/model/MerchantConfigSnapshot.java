package com.example.psp.analytics.domain.model;

public record MerchantConfigSnapshot(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps) {
}
