package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.UUID;

/**
 * One entry of {@code GET /api/payments/{id}/history}'s wire contract (M20; {@code
 * providerReference} added M21). Field order and names are load-bearing - the UI is built directly
 * against this exact shape:
 * {@code {"status": "...", "occurredAt": "...", "eventId": "...|null", "source": "...",
 * "providerReference": "...|null"}}. {@code eventId}/{@code providerReference} are {@code null}
 * for the one synthetic {@code CREATED} entry every response carries, and {@code providerReference}
 * is also {@code null} for a plain PENDING entry (no provider call yet) - Jackson serializes a
 * {@code null} field as JSON {@code null}, never an omitted key - see
 * {@code domain.model.PaymentHistoryItem}'s javadoc.
 */
public record PaymentHistoryItemResponse(
        String status, Instant occurredAt, UUID eventId, String source, String providerReference) {
}
