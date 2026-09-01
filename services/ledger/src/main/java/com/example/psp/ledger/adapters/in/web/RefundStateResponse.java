package com.example.psp.ledger.adapters.in.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record RefundStateResponse(
        UUID refundId,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String reason,
        Instant createdAt,
        Instant updatedAt) {
}
