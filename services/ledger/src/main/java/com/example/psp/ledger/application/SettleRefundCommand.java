package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.Money;
import java.util.UUID;

public record SettleRefundCommand(
        UUID inboundEventId,
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        String providerReference,
        String traceId,
        String correlationId) {
}
