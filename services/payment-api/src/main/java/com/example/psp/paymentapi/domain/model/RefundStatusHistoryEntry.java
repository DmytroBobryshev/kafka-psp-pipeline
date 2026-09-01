package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * One persisted row of the M23 refund trail ({@code refund_status_history}, schema owned by
 * {@code db/migration/V12__create_refund_status_history_table.sql}) - the refund-path mirror of
 * {@link PaymentStatusHistoryEntry}. One row per {@code refunds.refund-status-changed.v1}
 * (PENDING/IPN_RECEIVED/VERIFIED), {@code refunds.refund-completed.v1} (COMPLETED),
 * {@code refunds.refund-failed.v1} (FAILED) or {@code refunds.funds-reserved.v1}
 * (FUNDS_RESERVED) event this service's listeners have received - history-only, never touching
 * {@link Refund}'s own REQUESTED-only state (see that class's javadoc).
 *
 * <p>{@code eventId} is each event's own envelope eventId - the table's UNIQUE dedup key (V12),
 * same convention as {@link PaymentStatusHistoryEntry}. Pure Java, no framework dependency
 * (ADR-0007).
 */
@Getter
public final class RefundStatusHistoryEntry {

    private final UUID id;
    private final UUID refundId;
    private final UUID paymentId;
    private final String status;

    // The provider's own reference for this attempt, or null - always null for FUNDS_RESERVED/
    // FAILED (neither event carries one) and for PENDING (no provider call yet on that stage).
    private final String providerReference;

    private final UUID eventId;

    // Domain event time (envelope.occurredAt) - when the producing service says the fact happened.
    private final Instant occurredAt;

    // When THIS service recorded the row - see PaymentStatusHistoryEntry#recordedAt's identical
    // reasoning for why this is distinct from occurredAt.
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

    /**
     * Creates a brand-new row about to be recorded via
     * {@code domain.port.RefundStatusHistoryRepository#tryRecord} - {@code recordedAt} stamped
     * {@code now()}.
     */
    public static RefundStatusHistoryEntry record(
            UUID refundId, UUID paymentId, String status, String providerReference, UUID eventId, Instant occurredAt) {
        return new RefundStatusHistoryEntry(
                UUID.randomUUID(), refundId, paymentId, status, providerReference, eventId, occurredAt, Instant.now());
    }

    /** Reconstitutes a row from persisted state - used by {@code adapters/out/persistence}. */
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
