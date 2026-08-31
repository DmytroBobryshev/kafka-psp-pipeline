package com.example.psp.webhooknotifier.adapters.in.web;

import com.example.psp.webhooknotifier.config.WebhookNotifierProperties;
import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import org.springframework.stereotype.Component;

/**
 * Web-boundary mapper for {@code GET /api/webhooks/deliveries} (M19): domain -&gt; DTO. A plain
 * class rather than a MapStruct {@code @Mapper} - the one deliberate exception every other web
 * mapper in this codebase does NOT need to make: {@link #toResponse} computes {@code url} from
 * injected {@link WebhookNotifierProperties}, which MapStruct's generated implementations have no
 * clean way to receive (its mappers are stateless by design), so a hand-written
 * {@code @Component} is simpler here than fighting the generator for one derived field.
 */
@Component
public class WebhookDeliveryWebMapper {

    private final WebhookNotifierProperties properties;

    public WebhookDeliveryWebMapper(WebhookNotifierProperties properties) {
        this.properties = properties;
    }

    public WebhookDeliveryResponse toResponse(WebhookDelivery delivery) {
        return new WebhookDeliveryResponse(
                delivery.id().toString(),
                delivery.eventType(),
                delivery.paymentId().toString(),
                delivery.refundId() == null ? null : delivery.refundId().toString(),
                delivery.merchantId(),
                resolveUrl(delivery.merchantId()),
                delivery.status(),
                delivery.attempts(),
                delivery.lastAttemptAt(),
                delivery.createdAt());
    }

    /**
     * Same {@code {merchantId}} substitution
     * {@code adapters.out.http.RestClientMerchantWebhookClient} performs via {@code RestClient}'s
     * URI-template expansion, done here by hand since this is a read path with no
     * {@code RestClient} in scope.
     */
    private String resolveUrl(String merchantId) {
        String path = properties.merchantClient().webhookPath().replace("{merchantId}", merchantId);
        return properties.merchantClient().baseUrl() + path;
    }
}
