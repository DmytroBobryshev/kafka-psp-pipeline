package com.example.psp.paymentapi.application;

import java.time.Instant;
import java.util.UUID;

/**
 * Application-layer input model for {@link RecordRefundHistoryUseCase} (M23). One shared command
 * for all four refund-trail listeners (refund-status-changed, refund-completed, refund-failed,
 * funds-reserved) - every one of them is doing the exact same thing, an unconditional history-only
 * insert, differing only in which raw status string and providerReference they carry.
 *
 * @param refundId          the refund this row is about.
 * @param paymentId         the payment being refunded - carried through for the history endpoint.
 * @param status            the row's status: {@code PENDING}/{@code IPN_RECEIVED}/
 *                          {@code VERIFIED} (refund-status-changed), {@code COMPLETED}
 *                          (refund-completed), {@code FAILED} (refund-failed), or
 *                          {@code FUNDS_RESERVED} (funds-reserved) - this service's own literal,
 *                          that event carries no status field.
 * @param providerReference the provider's own reference, or {@code null} - never present for
 *                          FUNDS_RESERVED/FAILED (neither event carries one).
 * @param eventId           the envelope's own eventId - the {@code refund_status_history} dedup key.
 * @param occurredAt        the envelope's domain event time.
 */
public record RecordRefundHistoryCommand(
        UUID refundId, UUID paymentId, String status, String providerReference, UUID eventId, Instant occurredAt) {
}
