package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.util.List;

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
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        // M22: already resolved to a concrete value by the web mapper - absent-in-the-request ->
        // MerchantConfig.DEFAULT_PAYMENT_EXPIRATION_SECONDS - so this command, like every other
        // field here, carries a value ready to hand straight to the MerchantConfig constructor.
        int paymentExpirationSeconds) {
}
