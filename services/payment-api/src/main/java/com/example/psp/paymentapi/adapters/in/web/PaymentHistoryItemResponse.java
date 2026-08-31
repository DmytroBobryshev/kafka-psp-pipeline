package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.UUID;

/**
 * One entry of {@code GET /api/payments/{id}/history}'s wire contract (M20). Field order and
 * names are load-bearing - the UI is built directly against this exact shape:
 * {@code {"status": "...", "occurredAt": "...", "eventId": "...|null", "source": "..."}}.
 * {@code eventId} is {@code null} for the one synthetic {@code CREATED} entry every response
 * carries (Jackson serializes a {@code null} {@link UUID} as JSON {@code null}, not an omitted
 * field) - see {@code domain.model.PaymentHistoryItem}'s javadoc.
 */
public record PaymentHistoryItemResponse(String status, Instant occurredAt, UUID eventId, String source) {
}
