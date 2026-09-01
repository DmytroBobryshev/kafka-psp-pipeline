package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

@Getter
public final class PaymentStatusHistoryEntry {

    private final UUID id;
    private final UUID paymentId;
    private final String status;

    private final String providerReference;

    private final UUID eventId;

    // Domain event time (envelope.occurredAt) - when psp-connector says the outcome happened.
    private final Instant occurredAt;

    private final Instant recordedAt;

    private PaymentStatusHistoryEntry(
            UUID id,
            UUID paymentId,
            String status,
            String providerReference,
            UUID eventId,
            Instant occurredAt,
            Instant recordedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.providerReference = providerReference; // nullable - see field javadoc
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.recordedAt = Objects.requireNonNull(recordedAt, "recordedAt must not be null");
    }

    public static PaymentStatusHistoryEntry record(
            UUID paymentId, String status, String providerReference, UUID eventId, Instant occurredAt) {
        return new PaymentStatusHistoryEntry(
                UUID.randomUUID(), paymentId, status, providerReference, eventId, occurredAt, Instant.now());
    }

    public static PaymentStatusHistoryEntry reconstitute(
            UUID id,
            UUID paymentId,
            String status,
            String providerReference,
            UUID eventId,
            Instant occurredAt,
            Instant recordedAt) {
        return new PaymentStatusHistoryEntry(
                id, paymentId, status, providerReference, eventId, occurredAt, recordedAt);
    }
}
