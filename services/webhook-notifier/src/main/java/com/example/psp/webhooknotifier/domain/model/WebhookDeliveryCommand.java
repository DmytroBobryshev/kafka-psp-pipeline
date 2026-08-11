package com.example.psp.webhooknotifier.domain.model;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * The domain payload of one webhook delivery command - what {@code application.PlanWebhookDeliveryUseCase}
 * builds from an inbound {@code payments.payment-status-changed.v1} event, publishes to
 * {@code webhooks.webhook-delivery-requested.v1}, and what {@code application.ExecuteWebhookDeliveryUseCase}
 * eventually turns into an HTTP call via {@code domain.port.MerchantWebhookClient}.
 *
 * <p>Unlike {@code EventEnvelope}-carrying wire records (e.g. psp-connector's
 * {@code PaymentStatusChanged}), this is a pure domain type: no envelope, no Kafka headers. The
 * envelope/header concerns - causation id, trace id, correlation id, and everything the retry
 * chain needs (attempt count, original coordinates, exception info) - live in
 * {@link RetryEnvelope}, kept deliberately separate so this record can be reused unchanged
 * whether it arrived fresh from the planner or as the Nth redelivery from a retry topic.
 *
 * @param paymentId     the payment this status change is about.
 * @param merchantId    the owning merchant - the Kafka record key on every hop of the delivery
 *                      chain (ADR-0003) and the path segment
 *                      {@code adapters.out.http.RestClientMerchantWebhookClient} calls.
 * @param amount        the payment amount, echoed to the merchant so they don't have to look it
 *                      up.
 * @param currency      ISO 4217 currency code.
 * @param status        {@code "SUCCEEDED"} or {@code "DECLINED"} - mirrors
 *                      {@code payments.payment-status-changed.v1}'s own vocabulary unchanged; a
 *                      merchant cares about both.
 * @param declineReason populated only when {@code status = "DECLINED"}, else {@code null}.
 * @param causationEventId the {@code eventId} of the {@code payments.payment-status-changed.v1}
 *                      event that caused this delivery - carried through to the outbound event's
 *                      {@code EventEnvelope.causationId} (ADR-0002).
 * @param traceId       W3C trace-id, propagated end to end (ADR-0002).
 * @param correlationId the originating request id.
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
        String correlationId) {}
