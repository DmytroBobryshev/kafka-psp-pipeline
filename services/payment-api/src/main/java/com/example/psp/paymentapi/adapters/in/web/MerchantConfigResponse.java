package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

public record MerchantConfigResponse(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        int paymentExpirationSeconds,
        int refundExpirationSeconds) {
}
