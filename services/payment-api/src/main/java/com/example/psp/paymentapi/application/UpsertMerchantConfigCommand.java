package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.util.List;

public record UpsertMerchantConfigCommand(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        int paymentExpirationSeconds,
        int refundExpirationSeconds) {
}
