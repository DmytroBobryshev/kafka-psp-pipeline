package com.example.psp.webhooknotifier.domain.port;

import java.time.Instant;
import java.util.Optional;

/**
 * Outbound port for the {@code merchant_webhooks} projection: this service's own mirror of
 * {@code merchants.merchant-config-changed.v1}, scoped to the one field the delivery path needs -
 * {@code webhookUrl}. Implemented by {@code adapters.out.persistence.MongoMerchantWebhookDirectory};
 * written by {@code adapters.in.kafka.MerchantConfigChangedListener} via
 * {@code application.MerchantWebhookProjectionUseCase}; read by
 * {@code adapters.out.http.RestClientMerchantWebhookClient} at delivery time via
 * {@code domain.model.WebhookUrlResolver} - the bug fix this port exists for: a delivery command
 * carries no webhookUrl of its own (see {@code domain.model.WebhookDeliveryCommand}), so without
 * this projection the executor had no way to reach a merchant's real endpoint at all.
 */
public interface MerchantWebhookDirectory {

    /**
     * Upserts the merchant's current webhookUrl. {@code webhookUrl} may itself be {@code null} -
     * a merchant can be configured with no webhook - which is a different state from "no
     * projection at all" ({@link #delete}); both resolve to the simulated fallback at delivery
     * time, but only the latter is a tombstone.
     */
    void upsert(String merchantId, String webhookUrl, Instant updatedAt);

    /** Removes the merchant's projection entirely - the tombstone case (null Avro value). */
    void delete(String merchantId);

    /** The merchant's currently configured webhookUrl, or empty if never configured, blank, or
     * since deleted. */
    Optional<String> findWebhookUrl(String merchantId);
}
