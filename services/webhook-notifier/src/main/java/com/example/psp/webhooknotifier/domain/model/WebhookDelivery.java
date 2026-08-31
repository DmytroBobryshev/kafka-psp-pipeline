package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * M19: one row of the deliveries-visibility read model - {@code GET /api/webhooks/deliveries}
 * (see {@code adapters.in.web.WebhookDeliveryQueryController}). Pure Java, no MongoDB/Spring
 * dependency (ADR-0007) - this is the aggregate view {@code domain.port.DeliveryAttemptLogRepository#search}
 * builds by grouping the append-only {@link DeliveryAttempt} log by {@code causationEventId}: ONE
 * row per logical notification (however many retry-tier attempts it took), not one row per
 * {@link DeliveryAttempt} document.
 *
 * @param id            the grouping key - {@code causationEventId} of the event that caused this
 *                       delivery, stable across every retry-tier attempt of the same notification.
 * @param eventType      {@code "PAYMENT_STATUS_CHANGED"}, {@code "REFUND_COMPLETED"}, or
 *                       {@code "REFUND_FAILED"} - see {@link WebhookDeliveryCommand#eventType()}.
 * @param paymentId      the payment this delivery is about.
 * @param refundId       the refund this delivery is about, or {@code null} for a payment
 *                       status-change notification.
 * @param merchantId     the owning merchant.
 * @param status         the OUTCOME of the most recent attempt - {@code SUCCESS},
 *                       {@code RETRYABLE_FAILURE} (still mid-chain), or
 *                       {@code NON_RETRYABLE_FAILURE}/exhausted-to-DLQ.
 * @param attempts       how many attempts have been made for this delivery so far.
 * @param lastAttemptAt  when the most recent attempt happened.
 * @param createdAt      when the FIRST attempt happened - i.e. when this delivery was planned.
 */
public record WebhookDelivery(
        UUID id,
        String eventType,
        UUID paymentId,
        UUID refundId,
        String merchantId,
        String status,
        int attempts,
        Instant lastAttemptAt,
        Instant createdAt) {}
