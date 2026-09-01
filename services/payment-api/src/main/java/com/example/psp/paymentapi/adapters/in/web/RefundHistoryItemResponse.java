package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.UUID;

/**
 * One entry of {@code GET /api/payments/{paymentId}/refunds/{refundId}/history}'s wire contract
 * (M23). Field order and names are load-bearing - the UI is built directly against this exact
 * shape: {@code {"status": "...", "occurredAt": "...", "eventId": "...|null", "source": "...",
 * "providerReference": "...|null"}} - deliberately identical field set to
 * {@link PaymentHistoryItemResponse}.
 */
public record RefundHistoryItemResponse(
        String status, Instant occurredAt, UUID eventId, String source, String providerReference) {
}
