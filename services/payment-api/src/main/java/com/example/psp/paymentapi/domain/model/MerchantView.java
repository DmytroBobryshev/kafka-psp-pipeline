package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Local, eventually-consistent read projection of one merchant's config. System of record is
 * {@code merchants.merchant-config-changed.v1}; this is a mirror of it, maintained by
 * {@code adapters.in.kafka.MerchantConfigChangedListener}. Backs {@code GET /api/merchants} and
 * {@code CreatePaymentUseCase}'s onboarding gate.
 */
public record MerchantView(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        Instant updatedAt) {

    public MerchantView {
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(displayName, "displayName must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(payoutCurrency, "payoutCurrency must not be null");
        Objects.requireNonNull(allowedCurrencies, "allowedCurrencies must not be null");
        allowedCurrencies = List.copyOf(allowedCurrencies);
        // Unlike MerchantConfig, empty here is a legitimate value: it is the projection's
        // reading of a legacy (pre-M19) topic record, and CreatePaymentUseCase's gate falls back
        // to payoutCurrency for exactly that case - see the class javadoc.
        // webhookUrl is optional - a merchant may not have configured one.
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}
