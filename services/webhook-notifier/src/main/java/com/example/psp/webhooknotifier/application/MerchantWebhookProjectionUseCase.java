package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.port.MerchantWebhookDirectory;
import java.time.Instant;
import org.springframework.stereotype.Service;

@Service
public class MerchantWebhookProjectionUseCase {

    private final MerchantWebhookDirectory directory;

    public MerchantWebhookProjectionUseCase(MerchantWebhookDirectory directory) {
        this.directory = directory;
    }

    public void applyUpsert(String merchantId, String webhookUrl, Instant updatedAt) {
        directory.upsert(merchantId, webhookUrl, updatedAt);
    }

    public void applyDelete(String merchantId) {
        directory.delete(merchantId);
    }
}
