package com.example.psp.paymentapi.application;

import java.time.Instant;
import java.util.UUID;

public record RecordRefundHistoryCommand(
        UUID refundId, UUID paymentId, String status, String providerReference, UUID eventId, Instant occurredAt) {
}
