package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * One entry in a payment's status trail as {@code GET /api/payments/{id}/history} renders it
 * (M20) - the read-side view {@code application.PaymentQueryUseCase#history} assembles, ordered
 * {@code occurredAt} ascending, from two sources:
 *
 * <ul>
 *   <li>exactly one synthetic {@code CREATED} entry, built from the {@link Payment} row itself
 *       ({@code createdAt}, {@code eventId = null}, {@code source = "payment-api"}) - this
 *       service never persists a {@code CREATED} row in {@code payment_status_history} (nothing
 *       ever publishes a {@code payments.payment-status-changed.v1} event for it; see
 *       {@code db/migration/V9}'s comment), so the entry has to be synthesized here instead of
 *       read back from a table;
 *   <li>zero or more entries from {@code domain.port.PaymentStatusHistoryRepository#findByPaymentId}
 *       ({@code eventId} always present, {@code source = "psp-connector"} - the sole publisher of
 *       {@code payments.payment-status-changed.v1}, so no stored "who sent this" column is needed;
 *       the value is a constant this use case supplies).
 * </ul>
 *
 * <p>Deliberately NOT {@link PaymentStatusHistoryEntry} (the write-side, persisted-row shape,
 * which has no {@code source} and never includes a synthetic {@code CREATED} row) - this is a
 * read-side projection assembled on the way out, not an aggregate.
 *
 * <p>{@code status} is already this table's vocabulary (never the wire's {@code "DECLINED"}) -
 * see {@link PaymentStatusHistoryEntry}'s javadoc for where that translation happens. There is
 * deliberately no separate {@code rawStatus} field carrying the original wire spelling: by the
 * time a status reaches this class (via {@code adapters.in.kafka.PaymentStatusChangedMapper}),
 * {@code "DECLINED"} has already become {@link PaymentStatus#FAILED} and the original string is
 * gone - there is nothing left to carry.
 */
public record PaymentHistoryItem(PaymentStatus status, Instant occurredAt, UUID eventId, String source) {
}
