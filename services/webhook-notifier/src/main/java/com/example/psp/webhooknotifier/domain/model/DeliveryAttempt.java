package com.example.psp.webhooknotifier.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the MongoDB delivery-attempt log (M8 requirement #6): every single call to the
 * merchant's webhook, whichever tier of the retry chain it happened on. Written by
 * {@code application.ExecuteWebhookDeliveryUseCase} via {@code domain.port.DeliveryAttemptLogRepository}
 * immediately after {@code domain.port.MerchantWebhookClient#deliver} returns - success or
 * failure, every attempt is logged, not just failures, so the collection is a complete audit
 * trail of "what did we try, and what happened" for a given payment.
 *
 * @param merchantId    owning merchant.
 * @param paymentId     the payment this delivery is about.
 * @param attemptNumber 1 for the first attempt (base topic), 2/3/4 for each retry-tier hop.
 * @param outcome       ADR-0006 classification of this specific attempt.
 * @param statusCode    HTTP status received, or {@code null} if none was ever received (timeout,
 *                      connection refused).
 * @param error         human-readable failure detail, {@code null} on {@link DeliveryOutcome#SUCCESS}.
 * @param sourceTopic   which Kafka topic this attempt was consumed from - the base delivery topic
 *                      or one of the three retry tiers - so a Mongo query can show exactly how far
 *                      through the chain a payment's notification got.
 * @param attemptedAt   when this attempt was made; also the field the TTL index is built on (see
 *                      {@code adapters.out.persistence.DeliveryAttemptDocument}).
 */
public record DeliveryAttempt(
        String merchantId,
        UUID paymentId,
        int attemptNumber,
        DeliveryOutcome outcome,
        Integer statusCode,
        String error,
        String sourceTopic,
        Instant attemptedAt) {}
