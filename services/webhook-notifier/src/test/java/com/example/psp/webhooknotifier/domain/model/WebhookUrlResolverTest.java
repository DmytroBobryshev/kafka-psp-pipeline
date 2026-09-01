package com.example.psp.webhooknotifier.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class WebhookUrlResolverTest {

    private static final String FALLBACK = "/simulated-merchant/webhooks/{merchantId}";

    @Test
    void configuredWebhookUrlWinsOverTheFallback() {
        String resolved = WebhookUrlResolver.resolve(Optional.of("https://webhook.site/abc123"), FALLBACK);

        assertThat(resolved).isEqualTo("https://webhook.site/abc123");
    }

    @Test
    void noProjectedUrlFallsBackToTheSimulatedEndpoint() {
        String resolved = WebhookUrlResolver.resolve(Optional.empty(), FALLBACK);

        assertThat(resolved).isEqualTo(FALLBACK);
    }

    @Test
    void aTombstonedMerchantFallsBackToTheSimulatedEndpoint() {
        InMemoryMerchantWebhookDirectory directory = new InMemoryMerchantWebhookDirectory();
        directory.upsert("merchant-1", "https://webhook.site/abc123", Instant.parse("2026-01-01T00:00:00Z"));
        assertThat(WebhookUrlResolver.resolve(directory.findWebhookUrl("merchant-1"), FALLBACK))
                .isEqualTo("https://webhook.site/abc123");

        // The tombstone: the projection is removed entirely, not just nulled out.
        directory.delete("merchant-1");

        assertThat(WebhookUrlResolver.resolve(directory.findWebhookUrl("merchant-1"), FALLBACK))
                .isEqualTo(FALLBACK);
    }

    private static final class InMemoryMerchantWebhookDirectory implements MerchantWebhookDirectory {
        private final Map<String, String> byMerchantId = new HashMap<>();

        @Override
        public void upsert(String merchantId, String webhookUrl, Instant updatedAt) {
            byMerchantId.put(merchantId, webhookUrl);
        }

        @Override
        public void delete(String merchantId) {
            byMerchantId.remove(merchantId);
        }

        @Override
        public Optional<String> findWebhookUrl(String merchantId) {
            return Optional.ofNullable(byMerchantId.get(merchantId)).filter(url -> !url.isBlank());
        }
    }
}
