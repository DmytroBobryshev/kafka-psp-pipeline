package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

@Getter
public final class RefundStatusHistoryEntry {

    private final UUID id;
    private final UUID refundId;
    private final UUID paymentId;
    private final String status;

    private final String providerReference;

    private final UUID eventId;

    // Domain event time (envelope.occurredAt) - when the producing service says the fact happened.
    private final Instant occurredAt;

    private final Instant recordedAt;

    private RefundStatusHistoryEntry(
            UUID id,
            UUID refundId,
            UUID paymentId,
            String status,
            String providerReference,
            UUID eventId,
            Instant occurredAt,
            Instant recordedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.refundId = Objects.requireNonNull(refundId, "refundId must not be null");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.providerReference = providerReference; // nullable - see field javadoc
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.recordedAt = Objects.requireNonNull(recordedAt, "recordedAt must not be null");
    }

    public static RefundStatusHistoryEntry record(
            UUID refundId, UUID paymentId, String status, String providerReference, UUID eventId, Instant occurredAt) {
        return new RefundStatusHistoryEntry(
                UUID.randomUUID(), refundId, paymentId, status, providerReference, eventId, occurredAt, Instant.now());
    }

    public static RefundStatusHistoryEntry reconstitute(
            UUID id,
            UUID refundId,
            UUID paymentId,
            String status,
            String providerReference,
            UUID eventId,
            Instant occurredAt,
            Instant recordedAt) {
        return new RefundStatusHistoryEntry(
                id, refundId, paymentId, status, providerReference, eventId, occurredAt, recordedAt);
    }
}
