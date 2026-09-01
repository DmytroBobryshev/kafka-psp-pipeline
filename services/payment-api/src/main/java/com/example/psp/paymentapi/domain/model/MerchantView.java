package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record MerchantView(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        int paymentExpirationSeconds,
        int refundExpirationSeconds,
        Instant updatedAt) {

    public MerchantView {
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(displayName, "displayName must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(payoutCurrency, "payoutCurrency must not be null");
        Objects.requireNonNull(allowedCurrencies, "allowedCurrencies must not be null");
        allowedCurrencies = List.copyOf(allowedCurrencies);
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}
