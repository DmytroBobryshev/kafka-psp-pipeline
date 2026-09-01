package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.Money;
import java.util.UUID;

public record ExecuteRefundCommand(
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        UUID causationEventId,
        String traceId,
        String correlationId) {
}
