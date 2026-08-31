package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the MongoDB delivery-attempt log (M8 requirement #6): every single call to the
 * merchant's webhook, whichever tier of the retry chain it happened on. Written by
 * {@code application.ExecuteWebhookDeliveryUseCase} via {@code domain.port.DeliveryAttemptLogRepository}
 * immediately after {@code domain.port.MerchantWebhookClient#deliver} returns - success or
 * failure, every attempt is logged, not just failures, so the collection is a complete audit
 * trail of "what did we try, and what happened" for a given payment (or, since M19, a refund).
 *
 * <p>M19 adds {@link #eventType}, {@link #refundId}, and {@link #causationEventId} - mirroring
 * the same three additions to {@link WebhookDeliveryCommand}, for the same reason:
 * {@link #causationEventId} in particular is what
 * {@code adapters.out.persistence.MongoDeliveryAttemptLogRepository#search} groups rows by to
 * answer "one logical delivery" rather than "one HTTP attempt" - it is the one value that is
 * identical across every retry-tier hop of the same notification (the retry chain republishes the
 * command payload byte-for-byte unchanged - see {@code WebhookDeliveryRequested}'s Avro schema
 * doc) and unique to that notification otherwise, which neither {@code paymentId} alone (a
 * payment has exactly one status-change notification, but a payment's refund could in principle
 * be re-requested) nor {@code merchantId}+{@code paymentId} together reliably is.
 *
 * @param merchantId    owning merchant.
 * @param paymentId     the payment this delivery is about.
 * @param refundId      the refund this delivery is about, or {@code null} for a payment
 *                      status-change notification (M19).
 * @param eventType     which business event planned this delivery - see
 *                      {@link WebhookDeliveryCommand#eventType()} (M19).
 * @param causationEventId the {@code eventId} of the event that caused this delivery - the same
 *                      value on every retry-tier attempt of one logical delivery (M19).
 * @param attemptNumber 1 for the first attempt (base topic), 2/3/4 for each retry-tier hop.
 * @param outcome       ADR-0006 classification of this specific attempt.
 * @param statusCode    HTTP status received, or {@code null} if none was ever received (timeout,
 *                      connection refused).
 * @param error         human-readable failure detail, {@code null} on {@link DeliveryOutcome#SUCCESS}.
 * @param sourceTopic   which Kafka topic this attempt was consumed from - the base delivery topic
 *                      or one of the three retry tiers - so a Mongo query can show exactly how far
 *                      through the chain a notification got.
 * @param attemptedAt   when this attempt was made; also the field the TTL index is built on (see
 *                      {@code adapters.out.persistence.DeliveryAttemptDocument}).
 */
public record DeliveryAttempt(
        String merchantId,
        UUID paymentId,
        UUID refundId,
        String eventType,
        UUID causationEventId,
        int attemptNumber,
        DeliveryOutcome outcome,
        Integer statusCode,
        String error,
        String sourceTopic,
        Instant attemptedAt) {}
