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
 * <p>{@code status} is a plain {@code String} (M21 - was {@link PaymentStatus} through M20): the
 * synthetic {@code CREATED} entry carries {@link PaymentStatus#CREATED}{@code .name()}, and every
 * other entry carries {@link PaymentStatusHistoryEntry#getStatus()} verbatim - the event's raw wire
 * spelling, including {@code "IPN_RECEIVED"}/{@code "VERIFIED"}, which have no {@link PaymentStatus}
 * equivalent to translate into.
 *
 * @param providerReference the provider's own event id, or {@code null} - see
 *                           {@link PaymentStatusHistoryEntry#getProviderReference()}; always
 *                           {@code null} for the synthetic {@code CREATED} entry.
 */
public record PaymentHistoryItem(
        String status, Instant occurredAt, UUID eventId, String source, String providerReference) {
}
