package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.time.Instant;
import java.util.List;

public record UpsertMerchantViewCommand(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        List<String> allowedCurrencies,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        // M22: mirrors the Avro event's field of the same name (default 900 on the wire).
        int paymentExpirationSeconds,
        // M24: mirrors the Avro event's field of the same name (default 900 on the wire).
        int refundExpirationSeconds,
        Instant updatedAt) {
}
