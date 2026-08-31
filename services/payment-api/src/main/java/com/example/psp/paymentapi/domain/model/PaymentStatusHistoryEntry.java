package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * One persisted row of the M20 status trail ({@code payment_status_history}, schema owned by
 * {@code db/migration/V9__create_payment_status_history_table.sql}) - the write-side counterpart
 * to {@link Payment#getStatus()}'s single "current value" column. Where {@code payments.status}
 * only ever holds the latest outcome, this is one row per {@code payments.payment-status-changed.v1}
 * event {@code adapters.in.kafka.PaymentStatusChangedListener} has ever received and applied,
 * giving {@code GET /api/payments/{id}/history} the full PENDING -&gt; SUCCEEDED/FAILED trail, not
 * just the final state.
 *
 * <p>{@code eventId} is the envelope's own eventId (ADR-0002) - the table's UNIQUE dedup key (V9),
 * mirroring psp-connector's {@code payment_attempts.uq_payment_attempts_inbound_event_id}
 * convention for the identical "a redelivered event must not duplicate a row" problem. See
 * {@code domain.port.PaymentStatusHistoryRepository#tryRecord} for the insert-or-detect-duplicate
 * contract this identity backs.
 *
 * <p>{@code status} here is already this table's own vocabulary (never {@code "DECLINED"|"PENDING"}
 * the wire spelling) - the event-vocabulary translation happens once, upstream, in
 * {@code adapters.in.kafka.PaymentStatusChangedMapper#toPaymentStatus}, before a row ever reaches
 * this class or {@code application.ApplyPaymentOutcomeUseCase}.
 *
 * <p>Pure Java, no framework dependency (ADR-0007) - same identity-based-equality-by-convention
 * shape as {@link Payment}/{@link Refund} (equality is not implemented here since no caller needs
 * it; {@code id} would be the key if one is ever added).
 */
@Getter
public final class PaymentStatusHistoryEntry {

    private final UUID id;
    private final UUID paymentId;
    private final PaymentStatus status;
    private final UUID eventId;

    // Domain event time (envelope.occurredAt) - when psp-connector says the outcome happened.
    private final Instant occurredAt;

    // When THIS service recorded the row - distinct from occurredAt for the same reason
    // Payment#statusUpdatedAt is distinct from createdAt: publish-to-consume lag is a real,
    // observable gap, not noise to collapse away.
    private final Instant recordedAt;

    private PaymentStatusHistoryEntry(
            UUID id, UUID paymentId, PaymentStatus status, UUID eventId, Instant occurredAt, Instant recordedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.recordedAt = Objects.requireNonNull(recordedAt, "recordedAt must not be null");
    }

    /**
     * Creates a brand-new row about to be recorded via {@code
     * domain.port.PaymentStatusHistoryRepository#tryRecord} - {@code recordedAt} stamped
     * {@code now()}, mirroring {@code adapters.out.persistence.PostgresPaymentRepository
     * #updateStatus}'s "the clock is a persistence detail" reasoning for {@code statusUpdatedAt}.
     */
    public static PaymentStatusHistoryEntry record(
            UUID paymentId, PaymentStatus status, UUID eventId, Instant occurredAt) {
        return new PaymentStatusHistoryEntry(
                UUID.randomUUID(), paymentId, status, eventId, occurredAt, Instant.now());
    }

    /** Reconstitutes a row from persisted state - used by {@code adapters/out/persistence}. */
    public static PaymentStatusHistoryEntry reconstitute(
            UUID id, UUID paymentId, PaymentStatus status, UUID eventId, Instant occurredAt, Instant recordedAt) {
        return new PaymentStatusHistoryEntry(id, paymentId, status, eventId, occurredAt, recordedAt);
    }
}
