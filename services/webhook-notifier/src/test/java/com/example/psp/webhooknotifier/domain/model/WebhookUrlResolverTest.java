package com.example.psp.webhooknotifier.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The M8 bug fix, isolated to a plain JUnit test: no Spring, no Kafka, no HTTP, no MongoDB - same
 * "fakes, not a live broker" style as {@code ExecuteWebhookDeliveryUseCaseTest}. Covers the three
 * cases the resolution rule must get right:
 *
 * <ul>
 *   <li>a merchant's configured webhookUrl wins over the fallback;
 *   <li>no projection at all (never configured) falls back;
 *   <li>a tombstoned merchant (configured, then deleted) falls back too - proven through
 *       {@link InMemoryMerchantWebhookDirectory}, not just by handing {@link #resolve} an empty
 *       {@link Optional} directly, so the delete path itself is exercised.
 * </ul>
 */
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

    /** In-memory fake of {@link MerchantWebhookDirectory} - the same "fake port, not a mock"
     * convention {@code ExecuteWebhookDeliveryUseCaseTest}'s fakes already establish. */
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
