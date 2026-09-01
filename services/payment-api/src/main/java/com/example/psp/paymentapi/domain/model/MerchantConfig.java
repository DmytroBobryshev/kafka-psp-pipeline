package com.example.psp.paymentapi.domain.model;

import java.util.List;
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
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        int paymentExpirationSeconds,
        // M24: the refund-path mirror of paymentExpirationSeconds - see DEFAULT_REFUND_EXPIRATION_SECONDS.
        int refundExpirationSeconds) {

    /**
     * M22: the default a merchant gets when {@code PUT .../config}'s
     * {@code paymentExpirationSeconds} is absent, AND what the compacted topic's own Avro default
     * carries (06-merchant-config-changed.avsc) - kept as one named constant so the two never
     * drift independently. Also the value {@code adapters.out.persistence.PaymentJpaRepository}'s
     * expiration-candidate query falls back to (via {@code COALESCE}) for a merchant with no
     * {@code merchant_configs} row at all.
     */
    public static final int DEFAULT_PAYMENT_EXPIRATION_SECONDS = 900;

    /**
     * M24: the refund-path mirror of {@link #DEFAULT_PAYMENT_EXPIRATION_SECONDS} - same role, same
     * value, for {@code refundExpirationSeconds}: the default when {@code PUT .../config}'s field is
     * absent, the Avro field's own default, and what {@code adapters.out.persistence.RefundJpaRepository}'s
     * expiration-candidate query falls back to (via {@code COALESCE}) for a merchant with no
     * {@code merchant_configs} row at all.
     */
    public static final int DEFAULT_REFUND_EXPIRATION_SECONDS = 900;

    public MerchantConfig {
        requireNonBlank(merchantId, "merchantId");
        requireNonBlank(displayName, "displayName");
        Objects.requireNonNull(status, "status must not be null");
        requireNonBlank(payoutCurrency, "payoutCurrency");
        Objects.requireNonNull(allowedCurrencies, "allowedCurrencies must not be null");
        allowedCurrencies = List.copyOf(allowedCurrencies);
        // 1..3 currencies, and settlement must be one of them - a merchant that cannot be paid in
        // its own payout currency is a contradiction the compacted snapshot must never carry.
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
        // M22: 30s..24h - the web DTO enforces the same bounds (adapters.in.web.
        // UpsertMerchantConfigRequest), and this is the redundant-by-design layer a bug bypassing
        // that DTO cannot get around (same pattern as declineRateAlertThresholdBps above).
        if (paymentExpirationSeconds < 30 || paymentExpirationSeconds > 86_400) {
            throw new IllegalArgumentException(
                    "paymentExpirationSeconds must be within [30, 86400] seconds, was "
                            + paymentExpirationSeconds);
        }
        // M24: same 30s..24h bounds as paymentExpirationSeconds, same redundant-by-design layer
        // (adapters.in.web.UpsertMerchantConfigRequest enforces the same bounds at the DTO level).
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
