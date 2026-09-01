package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class MerchantViewProjectionUseCase {

    private static final Logger log = LoggerFactory.getLogger(MerchantViewProjectionUseCase.class);

    private final MerchantViewRepository merchantViewRepository;

    public MerchantViewProjectionUseCase(MerchantViewRepository merchantViewRepository) {
        this.merchantViewRepository = merchantViewRepository;
    }

    @Transactional
    public void applyUpsert(UpsertMerchantViewCommand command) {
        log.info(
                "Applying merchant-config upsert merchantId={} status={}",
                command.merchantId(),
                command.status());
        merchantViewRepository.upsert(
                new MerchantView(
                        command.merchantId(),
                        command.displayName(),
                        command.status(),
                        command.payoutCurrency(),
                        command.allowedCurrencies(),
                        command.webhookUrl(),
                        command.declineRateAlertThresholdBps(),
                        command.paymentExpirationSeconds(),
                        command.refundExpirationSeconds(),
                        command.updatedAt()));
    }

    @Transactional
    public void applyDelete(String merchantId) {
        log.info("Applying merchant-config tombstone merchantId={}", merchantId);
        merchantViewRepository.delete(merchantId);
    }
}
