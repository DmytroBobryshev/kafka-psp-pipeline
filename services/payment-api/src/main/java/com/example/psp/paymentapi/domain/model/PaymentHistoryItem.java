package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.UUID;

public record PaymentHistoryItem(
        String status, Instant occurredAt, UUID eventId, String source, String providerReference) {
}
