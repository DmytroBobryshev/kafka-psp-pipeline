package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.port.MerchantConfigPublisher;
import org.springframework.stereotype.Service;

@Service
public class MerchantConfigUseCase {

    private final MerchantConfigPublisher merchantConfigPublisher;

    public MerchantConfigUseCase(MerchantConfigPublisher merchantConfigPublisher) {
        this.merchantConfigPublisher = merchantConfigPublisher;
    }

    public MerchantConfig upsert(UpsertMerchantConfigCommand command) {
        MerchantConfig config =
                new MerchantConfig(
                        command.merchantId(),
                        command.displayName(),
                        command.status(),
                        command.payoutCurrency(),
                        command.allowedCurrencies(),
                        command.webhookUrl(),
                        command.declineRateAlertThresholdBps(),
                        command.paymentExpirationSeconds(),
                        command.refundExpirationSeconds());

        merchantConfigPublisher.publishConfigChanged(config);
        return config;
    }

    public void delete(String merchantId) {
        merchantConfigPublisher.publishConfigDeleted(merchantId);
    }
}
