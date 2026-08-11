package com.example.psp.analytics.adapters.in.web;

/**
 * Wire contract for {@code GET /api/analytics/merchants/{merchantId}/config} (M10) - a read of
 * the {@code GlobalKTable} row.
 *
 * <p>The endpoint's <b>404</b> is the interesting response, not this body: it is what a tombstone
 * looks like from the outside. {@code PUT} the config through payment-api and this returns 200
 * with the snapshot; {@code DELETE} it and, once the null-valued record reaches this instance's
 * global store, the same URL returns 404 - with no code anywhere in this service inspecting a
 * "deleted" field, because there is none.
 */
public record MerchantConfigResponse(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps) {
}
