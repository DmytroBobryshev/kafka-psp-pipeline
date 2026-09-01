package com.example.psp.webhooknotifier.adapters.out.persistence;

import com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory;
import java.time.Instant;
import java.util.Optional;
import org.springframework.stereotype.Repository;

@Repository
public class MongoMerchantWebhookDirectory implements MerchantWebhookDirectory {

    private final MerchantWebhookMongoRepository mongoRepository;

    public MongoMerchantWebhookDirectory(MerchantWebhookMongoRepository mongoRepository) {
        this.mongoRepository = mongoRepository;
    }

    @Override
    public void upsert(String merchantId, String webhookUrl, Instant updatedAt) {
        mongoRepository.save(
                MerchantWebhookDocument.builder()
                        .merchantId(merchantId)
                        .webhookUrl(webhookUrl)
                        .updatedAt(updatedAt)
                        .build());
    }

    @Override
    public void delete(String merchantId) {
        mongoRepository.deleteById(merchantId);
    }

    @Override
    public Optional<String> findWebhookUrl(String merchantId) {
        return mongoRepository
                .findById(merchantId)
                .map(MerchantWebhookDocument::getWebhookUrl)
                .filter(url -> url != null && !url.isBlank());
    }
}
