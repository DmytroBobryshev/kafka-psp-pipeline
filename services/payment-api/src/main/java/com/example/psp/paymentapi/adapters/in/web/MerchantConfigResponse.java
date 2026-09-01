package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

/**
 * Wire contract returned by {@code PUT /api/merchants/{merchantId}/config} (M10) - an echo of
 * what was published, so a caller can see the exact snapshot that became the topic's new
 * last-value for this key.
 *
 * <p>There is no {@code GET} counterpart on this service. Reading merchant config means reading
 * the compacted topic, and the services that need it already hold it in a {@code GlobalKTable}
 * (analytics exposes it at {@code GET /api/analytics/merchants/{merchantId}/config}, port 8089).
 * Adding a read endpoint here would mean either a second copy of the state in payment-api or a
 * service-to-service REST call, and ADR-0004 rules out the latter outright.
 */
public record MerchantConfigResponse(
        String merchantId,
        String displayName,
        String status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        int paymentExpirationSeconds,
        int refundExpirationSeconds) {
}
