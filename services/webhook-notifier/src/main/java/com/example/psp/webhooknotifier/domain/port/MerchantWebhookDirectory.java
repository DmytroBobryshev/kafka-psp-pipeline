package com.example.psp.webhooknotifier.domain.port;

import java.time.Instant;
import java.util.Optional;

public interface MerchantWebhookDirectory {

    void upsert(String merchantId, String webhookUrl, Instant updatedAt);

    void delete(String merchantId);

    Optional<String> findWebhookUrl(String merchantId);
}
