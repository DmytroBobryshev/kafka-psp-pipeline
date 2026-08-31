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
    private final com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory merchantWebhookDirectory;

    public WebhookDeliveryWebMapper(WebhookNotifierProperties properties,
            com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory merchantWebhookDirectory) {
        this.merchantWebhookDirectory = merchantWebhookDirectory;
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

    // Same resolution rule the delivery path uses - the displayed target must match the real one.
    private String resolveUrl(String merchantId) {
        String path = properties.merchantClient().webhookPath().replace("{merchantId}", merchantId);
        return com.example.psp.webhooknotifier.domain.model.WebhookUrlResolver.resolve(
                merchantWebhookDirectory.findWebhookUrl(merchantId),
                properties.merchantClient().baseUrl() + path);
    }
}
