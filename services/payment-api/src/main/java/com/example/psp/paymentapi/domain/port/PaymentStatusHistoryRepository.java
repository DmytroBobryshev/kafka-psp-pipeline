package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import java.util.List;
import java.util.UUID;

/**
 * Outbound port for the M20 status trail ({@code payment_status_history}). Implemented by
 * {@code adapters/out/persistence} - the domain depends only on this interface, never on JPA or
 * any concrete storage technology (ADR-0007).
 */
public interface PaymentStatusHistoryRepository {

    /**
     * Inserts one row, keyed by {@code entry.eventId()}'s UNIQUE constraint (V9) - the
     * DB-constraint-is-the-authority idempotent-insert convention psp-connector's
     * {@code AttemptLogRepository#tryRecord} already established for an analogous problem
     * (redelivery of the same inbound event must not duplicate a row).
     *
     * @return {@code true} if this call inserted the row; {@code false} if {@code eventId} was
     *     already recorded (a redelivered {@code payments.payment-status-changed.v1} event -
     *     harmless, not an error, never rethrown).
     */
    boolean tryRecord(PaymentStatusHistoryEntry entry);

    /**
     * Every recorded row for {@code paymentId}, ordered {@code occurredAt} ascending - the raw
     * material {@code application.PaymentQueryUseCase#history} merges with a synthetic
     * {@code CREATED} entry to answer {@code GET /api/payments/{id}/history}. Never includes a
     * {@code CREATED} row (see {@code domain.model.PaymentHistoryItem}'s javadoc for why); an
     * unknown {@code paymentId} answers an empty list, same "no rows yet" convention as
     * {@code RefundRepository#findByPaymentId}.
     */
    List<PaymentStatusHistoryEntry> findByPaymentId(UUID paymentId);
}
