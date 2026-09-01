package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import java.util.List;
import java.util.UUID;

/**
 * Outbound port for the M23 refund trail ({@code refund_status_history}) - the refund-path mirror
 * of {@link PaymentStatusHistoryRepository}. Implemented by {@code adapters/out/persistence}; the
 * domain depends only on this interface (ADR-0007).
 */
public interface RefundStatusHistoryRepository {

    /**
     * Inserts one row, keyed by {@code entry.eventId()}'s UNIQUE constraint (V12) - same
     * DB-constraint-is-the-authority idempotent-insert convention as
     * {@link PaymentStatusHistoryRepository#tryRecord}.
     *
     * @return {@code true} if this call inserted the row; {@code false} if {@code eventId} was
     *     already recorded (a redelivered event - harmless, never rethrown).
     */
    boolean tryRecord(RefundStatusHistoryEntry entry);

    /** Every recorded row for {@code refundId}, ordered {@code occurredAt} ascending. */
    List<RefundStatusHistoryEntry> findByRefundId(UUID refundId);
}
