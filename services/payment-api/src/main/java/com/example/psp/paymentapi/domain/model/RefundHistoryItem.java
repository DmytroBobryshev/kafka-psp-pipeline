package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * One entry in a refund's status trail as
 * {@code GET /api/payments/{paymentId}/refunds/{refundId}/history} renders it (M23) - the
 * refund-path mirror of {@link PaymentHistoryItem}. {@code application.PaymentQueryUseCase
 * #refundHistory} assembles this, ordered {@code occurredAt} ascending, from two sources:
 *
 * <ul>
 *   <li>exactly one synthetic {@code REQUESTED} entry, built from the {@link Refund} row itself
 *       ({@code createdAt}, {@code eventId = null}, {@code source = "payment-api"}) - this
 *       service never persists a REQUESTED row in {@code refund_status_history} (nothing ever
 *       publishes an event for the moment the refund was requested; the row itself IS that fact);
 *   <li>zero or more entries from
 *       {@code domain.port.RefundStatusHistoryRepository#findByRefundId}, {@code source}
 *       attributed by status: {@code FUNDS_RESERVED} -&gt; {@code "ledger"} (the sole publisher of
 *       {@code refunds.funds-reserved.v1}); every other status
 *       ({@code PENDING}/{@code IPN_RECEIVED}/{@code VERIFIED}/{@code COMPLETED}/{@code FAILED})
 *       -&gt; {@code "psp-connector"}, the sole publisher of those four.
 * </ul>
 *
 * @param providerReference the provider's own reference, or {@code null} - see
 *                           {@link RefundStatusHistoryEntry#getProviderReference()}; always
 *                           {@code null} for the synthetic REQUESTED entry.
 */
public record RefundHistoryItem(
        String status, Instant occurredAt, UUID eventId, String source, String providerReference) {
}
