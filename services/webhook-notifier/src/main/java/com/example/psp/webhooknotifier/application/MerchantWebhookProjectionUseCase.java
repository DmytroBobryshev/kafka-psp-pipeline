package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory;
import java.time.Instant;
import org.springframework.stereotype.Service;

/**
 * Projects {@code merchants.merchant-config-changed.v1} into the {@code merchant_webhooks} read
 * model ({@link MerchantWebhookDirectory}), consulted at delivery time by
 * {@code adapters.out.http.RestClientMerchantWebhookClient}. Mirrors payment-api's own
 * {@code MerchantConfigChangedListener} projection - a second, independent consumer group
 * ({@code webhook-notifier.merchant-view.v1}) over the same compacted topic (ADR-0005) - scoped
 * to just the one field this service needs.
 */
@Service
public class MerchantWebhookProjectionUseCase {

    private final MerchantWebhookDirectory directory;

    public MerchantWebhookProjectionUseCase(MerchantWebhookDirectory directory) {
        this.directory = directory;
    }

    /** Upsert case - a merchant's config was created or changed. */
    public void applyUpsert(String merchantId, String webhookUrl, Instant updatedAt) {
        directory.upsert(merchantId, webhookUrl, updatedAt);
    }

    /** Tombstone case - the merchant's config was deleted. */
    public void applyDelete(String merchantId) {
        directory.delete(merchantId);
    }
}
