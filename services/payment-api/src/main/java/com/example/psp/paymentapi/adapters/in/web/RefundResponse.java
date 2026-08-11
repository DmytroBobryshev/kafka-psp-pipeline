package com.example.psp.paymentapi.adapters.in.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Wire contract returned by {@code POST /api/payments/{paymentId}/refunds} (M11). {@code status}
 * is always {@code "REQUESTED"} - the response the sequence diagram (docs/diagrams/
 * sequence-refund-saga.md) calls "202 Accepted {refundId, status: REQUESTED}"; the saga's further
 * states are only visible in the ledger's {@code GET /api/refunds/{refundId}}
 * (services/ledger/README.md's M11 section).
 */
public record RefundResponse(
        UUID id, UUID paymentId, String merchantId, BigDecimal amount, String currency, String status, String reason, Instant createdAt) {
}
