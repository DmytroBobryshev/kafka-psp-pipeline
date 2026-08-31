package com.example.psp.webhooknotifier.domain.model;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * The domain payload of one webhook delivery command - what {@code application.PlanWebhookDeliveryUseCase}
 * builds from an inbound event, publishes to {@code webhooks.webhook-delivery-requested.v2}, and
 * what {@code application.ExecuteWebhookDeliveryUseCase} eventually turns into an HTTP call via
 * {@code domain.port.MerchantWebhookClient}.
 *
 * <p>Unlike {@code EventEnvelope}-carrying wire records (e.g. psp-connector's
 * {@code PaymentStatusChanged}), this is a pure domain type: no envelope, no Kafka headers. The
 * envelope/header concerns - causation id, trace id, correlation id, and everything the retry
 * chain needs (attempt count, original coordinates, exception info) - live in
 * {@link RetryEnvelope}, kept deliberately separate so this record can be reused unchanged
 * whether it arrived fresh from the planner or as the Nth redelivery from a retry topic.
 *
 * <h2>M19 - one shape, three source events</h2>
 *
 * <p>Through M9 Phase 2 this record only ever came from {@code payments.payment-status-changed.v1}.
 * M19 adds two more planner sources - {@code refunds.refund-completed.v1} and
 * {@code refunds.refund-failed.v1} (see {@code adapters.in.kafka.RefundCompletedMapper}/
 * {@code RefundFailedMapper}) - reusing this exact same shape rather than inventing a parallel
 * "refund delivery command" type, so the retry chain, the executor, the DLQ, and the attempt log
 * stay generic over "a notification to deliver" and do not need to know which kind of business
 * event produced it. {@link #eventType} is what tells them apart; {@link #refundId} is populated
 * only when {@link #eventType} names a refund event.
 *
 * @param paymentId     the payment this notification is about - always present: a refund is
 *                      always a refund OF a payment.
 * @param merchantId    the owning merchant - the Kafka record key on every hop of the delivery
 *                      chain (ADR-0003) and the path segment
 *                      {@code adapters.out.http.RestClientMerchantWebhookClient} calls.
 * @param amount        the payment or refund amount, echoed to the merchant so they don't have to
 *                      look it up.
 * @param currency      ISO 4217 currency code.
 * @param status        the source event's own outcome vocabulary, echoed unchanged: {@code
 *                      "SUCCEEDED"}/{@code "DECLINED"} for {@link #eventType}
 *                      {@code "PAYMENT_STATUS_CHANGED"}, {@code "COMPLETED"} for
 *                      {@code "REFUND_COMPLETED"}, {@code "FAILED"} for {@code "REFUND_FAILED"}.
 * @param declineReason populated when {@code status} names a failure - the payment decline reason
 *                      for a {@code DECLINED} payment, or the refund failure reason
 *                      ({@code "INSUFFICIENT_BALANCE"}/{@code "PROVIDER_DECLINED"}, see
 *                      {@code refunds.refund-failed.v1}'s schema doc) for a {@code FAILED}
 *                      refund - else {@code null}. The field keeps its original payment-only name
 *                      (a wire-schema rename is a bigger, non-additive change than reusing the
 *                      slot - see {@code libs/common-events}'s
 *                      {@code 05-webhook-delivery-requested.avsc}) but the meaning is now "why did
 *                      this notification report a failure", not literally "why was the card
 *                      declined".
 * @param causationEventId the {@code eventId} of the event that caused this delivery - carried
 *                      through to the outbound event's {@code EventEnvelope.causationId}
 *                      (ADR-0002).
 * @param traceId       W3C trace-id, propagated end to end (ADR-0002).
 * @param correlationId the originating request id.
 * @param eventType     which business event planned this delivery: {@code "PAYMENT_STATUS_CHANGED"},
 *                      {@code "REFUND_COMPLETED"}, or {@code "REFUND_FAILED"} (M19). Lets the
 *                      attempt log and the deliveries-visibility API
 *                      ({@code adapters.in.web.WebhookDeliveryQueryController}) tell a refund
 *                      notification apart from a payment one without inferring it from
 *                      {@link #status}'s overlapping vocabulary.
 * @param refundId      the refund this notification is about, or {@code null} when
 *                      {@link #eventType} is {@code "PAYMENT_STATUS_CHANGED"} (M19).
 */
public record WebhookDeliveryCommand(
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        UUID causationEventId,
        String traceId,
        String correlationId,
        String eventType,
        UUID refundId) {}
