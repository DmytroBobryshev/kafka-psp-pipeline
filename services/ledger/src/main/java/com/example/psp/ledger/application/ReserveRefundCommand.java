package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.Money;
import java.util.UUID;

public record ReserveRefundCommand(
        UUID inboundEventId,
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        String traceId,
        String correlationId) {
}
