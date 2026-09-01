package com.example.psp.webhooknotifier.domain.model;

import java.util.Optional;

public final class WebhookUrlResolver {

    private WebhookUrlResolver() {}

    public static String resolve(Optional<String> projectedWebhookUrl, String fallbackUri) {
        return projectedWebhookUrl.filter(url -> !url.isBlank()).orElse(fallbackUri);
    }
}
