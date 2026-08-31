package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.time.Instant;

/**
 * Application-layer input for {@link MerchantViewProjectionUseCase#applyUpsert}, mapped from the
 * inbound Avro event by {@code adapters.in.kafka.MerchantConfigChangedMapper} - the use case never
 * sees the wire type.
 */
public record UpsertMerchantViewCommand(
        String merchantId,
        String displayName,
        MerchantStatus status,
        String payoutCurrency,
        String webhookUrl,
        int declineRateAlertThresholdBps,
        Instant updatedAt) {
}
