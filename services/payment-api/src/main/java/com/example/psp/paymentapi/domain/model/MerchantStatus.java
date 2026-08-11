package com.example.psp.paymentapi.domain.model;

/**
 * Lifecycle state of a merchant's configuration (M10).
 *
 * <p>There is deliberately no {@code DELETED} constant. On {@code
 * merchants.merchant-config-changed.v1} - a {@code cleanup.policy=compact} topic - deletion is a
 * <b>tombstone</b>: a record with a {@code null} value under the merchant's key, not a record
 * whose value carries a "deleted" marker. Adding a {@code DELETED} status here would look
 * equivalent and would not be: compaction retains the last <i>value</i> per key, so a
 * {@code DELETED}-valued record is retained forever and every downstream {@code GlobalKTable}
 * keeps a row for a merchant that no longer exists. Only a null value makes compaction remove
 * the key. See {@link com.example.psp.paymentapi.domain.port.MerchantConfigPublisher} and
 * services/payment-api/README.md's "Merchant config" section.
 */
public enum MerchantStatus {

    /** Merchant may transact. */
    ACTIVE,

    /**
     * Merchant is on hold - config still exists and is still the last compacted value for the
     * key, so downstream lookups return it and can act on it. This is what a soft-disable looks
     * like when it is genuinely a state and not a deletion.
     */
    SUSPENDED
}
