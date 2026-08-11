package com.example.psp.ledger.adapters.in.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Wire contract for {@code GET /api/refunds/{refundId}} (M11 step 5). This is the ledger's own
 * local saga-state view (ADR-0008 rule 1) - not an aggregation across services. Records for DTOs,
 * per PLAN.md.
 */
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
