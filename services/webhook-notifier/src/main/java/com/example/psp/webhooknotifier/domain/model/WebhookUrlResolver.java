package com.example.psp.webhooknotifier.domain.model;

import java.util.Optional;

/**
 * THE bug fix: which URL {@code adapters.out.http.RestClientMerchantWebhookClient} actually
 * calls. Resolved AT DELIVERY TIME, not at planning time - {@link WebhookDeliveryCommand} carries
 * no webhookUrl of its own, so a merchant's most recently projected configuration
 * ({@code domain.port.MerchantWebhookDirectory}) always wins over whatever was (or was not)
 * configured when the delivery was planned, including a webhookUrl set after planning but before
 * this attempt runs.
 */
public final class WebhookUrlResolver {

    private WebhookUrlResolver() {}

    /**
     * @param projectedWebhookUrl the merchant's currently projected webhookUrl - empty when the
     *                            merchant never configured one, was tombstoned, or the projection
     *                            has not caught up yet.
     * @param fallbackUri         the built-in simulated-merchant endpoint
     *                            ({@code webhook-notifier.merchant-client.webhook-path}, resolved
     *                            against {@code webhook-notifier.merchant-client.base-url}) -
     *                            never null.
     * @return the merchant's configured URL if present and non-blank, else {@code fallbackUri}.
     */
    public static String resolve(Optional<String> projectedWebhookUrl, String fallbackUri) {
        return projectedWebhookUrl.filter(url -> !url.isBlank()).orElse(fallbackUri);
    }
}
