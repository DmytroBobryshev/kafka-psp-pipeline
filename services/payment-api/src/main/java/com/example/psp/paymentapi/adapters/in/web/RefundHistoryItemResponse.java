package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.UUID;

public record RefundHistoryItemResponse(
        String status, Instant occurredAt, UUID eventId, String source, String providerReference) {
}
