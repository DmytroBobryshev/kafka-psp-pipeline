package com.example.psp.paymentapi.domain.model;

import java.util.Objects;

/**
 * The merchant configuration aggregate (M10). Pure Java, no framework dependency (ADR-0007).
 *
 * <p>Unlike {@link Payment}, this aggregate has <b>no database row anywhere in this service</b>.
 * Its system of record is the compacted topic {@code merchants.merchant-config-changed.v1}: log
 * compaction guarantees "the last value for every key is retained forever", which is exactly the
 * durability contract a key/value table gives you, so a second copy in Postgres would be a
 * derived replica that has to be reconciled rather than a source of truth. That single fact is
 * also why this write path does not use the M6 outbox - see
 * {@link com.example.psp.paymentapi.domain.port.MerchantConfigPublisher}.
 *
 * <p>A {@code MerchantConfig} instance is always a <b>whole-state snapshot</b>, never a delta.
 * Compaction keeps the last record per key and discards everything before it, so a consumer that
 * starts reading at a compacted offset sees exactly one record per merchant and must be able to
 * reconstruct the full configuration from it alone. Partial updates ("just change the webhook
 * URL") are therefore expressed by re-sending every field, which is why the REST surface is
 * {@code PUT} (replace) and not {@code PATCH} (merge).
 */
public record MerchantConfig(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps) {

    public MerchantConfig {
        requireNonBlank(merchantId, "merchantId");
        requireNonBlank(displayName, "displayName");
        Objects.requireNonNull(status, "status must not be null");
        requireNonBlank(payoutCurrency, "payoutCurrency");
        // webhookUrl is the one genuinely optional field - a merchant may have none.
        if (declineRateAlertThresholdBps < 0 || declineRateAlertThresholdBps > 10_000) {
            throw new IllegalArgumentException(
                    "declineRateAlertThresholdBps must be within [0, 10000] (basis points of a whole)");
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
