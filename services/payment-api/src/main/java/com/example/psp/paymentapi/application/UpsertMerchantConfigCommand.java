package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantStatus;

/**
 * Application-layer input model for {@link MerchantConfigUseCase#upsert} (M10). Deliberately
 * separate from the web DTO in {@code adapters/in/web}, same reasoning as
 * {@link CreatePaymentCommand}.
 *
 * <p>Carries every configuration field, not just the changed ones, because the target topic is
 * compacted: see {@link com.example.psp.paymentapi.domain.model.MerchantConfig} on why a
 * whole-state snapshot is the only shape that survives compaction.
 */
public record UpsertMerchantConfigCommand(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps) {
}
