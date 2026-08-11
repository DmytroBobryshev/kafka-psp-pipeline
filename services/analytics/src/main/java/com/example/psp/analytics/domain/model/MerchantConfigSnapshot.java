package com.example.psp.analytics.domain.model;

/**
 * The merchant configuration as this service currently holds it, read out of the
 * {@code GlobalKTable} backed by the compacted {@code merchants.merchant-config-changed.v1} topic
 * (M10). Pure Java (ADR-0007) - the Avro {@code MerchantConfigChanged} record never escapes
 * {@code adapters/}.
 *
 * <p>A lookup that returns {@code null} instead of an instance of this type is the observable
 * effect of a <b>tombstone</b>: payment-api published a record with this merchant's key and a
 * null value, Streams removed the row from the table, and the store no longer has anything to
 * return. Nothing in this service checks a "deleted" flag, because there is no flag to check -
 * that is precisely the property a tombstone buys over a soft-delete field.
 */
public record MerchantConfigSnapshot(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps) {
}
