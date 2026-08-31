package com.example.psp.webhooknotifier.adapters.in.web;

import java.time.Instant;

/**
 * Wire contract returned by {@code GET /api/webhooks/deliveries} (M19, deliveries visibility).
 * Field names are load-bearing - the UI is built directly against this exact shape. One row per
 * logical delivery (every retry-tier attempt folded together by {@code causationEventId} - see
 * {@code domain.model.WebhookDelivery}), not one row per HTTP attempt.
 *
 * @param id            the delivery's stable id ({@code causationEventId} of the event that
 *                      caused it), as a string.
 * @param eventType     {@code "PAYMENT_STATUS_CHANGED"}, {@code "REFUND_COMPLETED"}, or
 *                      {@code "REFUND_FAILED"}.
 * @param paymentId     the payment this delivery is about.
 * @param refundId      the refund this delivery is about, or {@code null} for a payment
 *                      status-change delivery.
 * @param merchantId    the owning merchant.
 * @param url           where this delivery was (or would be) sent - derived at read time from
 *                      {@code webhook-notifier.merchant-client.base-url}/{@code webhook-path} and
 *                      {@code merchantId}, NOT stored per-attempt: the destination is stable
 *                      per-merchant configuration, not a fact about any one HTTP call, so
 *                      recomputing it here is more honest than persisting a copy that could drift
 *                      from config after the fact.
 * @param status        the outcome of the most recent attempt - {@code SUCCESS},
 *                      {@code RETRYABLE_FAILURE} (still mid-chain), or
 *                      {@code NON_RETRYABLE_FAILURE}.
 * @param attempts      how many attempts have been made for this delivery so far.
 * @param lastAttemptAt when the most recent attempt happened.
 * @param createdAt     when this delivery was first planned (its first attempt).
 */
public record WebhookDeliveryResponse(
        String id,
        String eventType,
        String paymentId,
        String refundId,
        String merchantId,
        String url,
        String status,
        int attempts,
        Instant lastAttemptAt,
        Instant createdAt) {}
