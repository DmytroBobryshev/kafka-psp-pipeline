package com.example.psp.webhooknotifier.adapters.in.kafka;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wire shape of {@code webhooks.webhook-delivery-requested.v1} (and its three retry-tier mirrors)
 * as the executor deserializes it - the consuming side's mirror of
 * {@code adapters.out.kafka.WebhookDeliveryRequested}, the producer-side record the planner and
 * the retry-hop publisher both write. Deliberately a SEPARATE class from that one, even though
 * both live inside this one service and share every field: {@code adapters.in} must never depend
 * on {@code adapters.out} or vice versa (ADR-0007's hexagon rule, enforced by
 * {@code architecture.HexagonalArchitectureTest}), the same as it would be if planner and executor
 * were two different services.
 *
 * <p>No {@code EventEnvelope} here on purpose: unlike a business-event topic
 * (ADR-0002), this is an internal command/work-item topic, and the retry metadata that would
 * otherwise live in an envelope (attempt count, causing exception, original coordinates) travels
 * as Kafka HEADERS instead ({@code domain.model.RetryHeaderNames}), read directly by
 * {@code adapters.in.kafka.WebhookDeliveryExecutorListener} via {@code @Header} - headers, not the
 * record value, are what a retry-topic redelivery actually needs to carry, and keeping them out
 * of the JSON body means a replayed/retried record's PAYLOAD is byte-for-byte identical on every
 * hop, which is exactly what M8's replay/redelivery story needs to be true.
 */
public record WebhookDeliveryRequestedEvent(
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        UUID causationEventId,
        String traceId,
        String correlationId) {}
