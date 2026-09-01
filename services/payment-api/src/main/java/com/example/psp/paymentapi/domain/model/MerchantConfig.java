package com.example.psp.paymentapi.domain.model;

import java.util.List;
import java.util.Objects;

public record MerchantConfig(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        int paymentExpirationSeconds,
        // M24: the refund-path mirror of paymentExpirationSeconds - see DEFAULT_REFUND_EXPIRATION_SECONDS.
        int refundExpirationSeconds) {

    public static final int DEFAULT_PAYMENT_EXPIRATION_SECONDS = 900;

    public static final int DEFAULT_REFUND_EXPIRATION_SECONDS = 900;

    public MerchantConfig {
        requireNonBlank(merchantId, "merchantId");
        requireNonBlank(displayName, "displayName");
        Objects.requireNonNull(status, "status must not be null");
        requireNonBlank(payoutCurrency, "payoutCurrency");
        Objects.requireNonNull(allowedCurrencies, "allowedCurrencies must not be null");
        allowedCurrencies = List.copyOf(allowedCurrencies);
        if (allowedCurrencies.isEmpty() || allowedCurrencies.size() > 3) {
            throw new IllegalArgumentException(
                    "allowedCurrencies must contain 1 to 3 currency codes, got " + allowedCurrencies);
        }
        if (!allowedCurrencies.contains(payoutCurrency)) {
            throw new IllegalArgumentException(
                    "payoutCurrency "
                            + payoutCurrency
                            + " must be one of allowedCurrencies "
                            + allowedCurrencies
                            + " - a merchant must be able to settle in a currency it accepts");
        }
        // webhookUrl is the one genuinely optional field - a merchant may have none.
        if (declineRateAlertThresholdBps < 0 || declineRateAlertThresholdBps > 10_000) {
            throw new IllegalArgumentException(
                    "declineRateAlertThresholdBps must be within [0, 10000] (basis points of a whole)");
        }
        if (paymentExpirationSeconds < 30 || paymentExpirationSeconds > 86_400) {
            throw new IllegalArgumentException(
                    "paymentExpirationSeconds must be within [30, 86400] seconds, was "
                            + paymentExpirationSeconds);
        }
        if (refundExpirationSeconds < 30 || refundExpirationSeconds > 86_400) {
            throw new IllegalArgumentException(
                    "refundExpirationSeconds must be within [30, 86400] seconds, was "
                            + refundExpirationSeconds);
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
