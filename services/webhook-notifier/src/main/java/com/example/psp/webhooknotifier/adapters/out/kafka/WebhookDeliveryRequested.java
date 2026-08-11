package com.example.psp.webhooknotifier.adapters.out.kafka;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wire shape of {@code webhooks.webhook-delivery-requested.v1} (and its three retry-tier
 * mirrors) as this service WRITES it - used by {@code KafkaWebhookDeliveryPublisher} for every
 * publish (the planner's first publish, a retry hop, a DLQ publish, and a DLQ replay's
 * republish), and reused by {@code KafkaDlqReader} to decode a DLQ record's value, since both
 * classes live in this same {@code adapters.out.kafka} package (never {@code adapters.in.kafka} -
 * see that package's mirror, {@code WebhookDeliveryRequestedEvent}, for why the two are separate
 * types even though every field matches).
 *
 * <p>No {@code EventEnvelope}: see {@code adapters.in.kafka.WebhookDeliveryRequestedEvent}'s
 * javadoc for why this is an internal command/work-item topic, not a business-event one, and why
 * the retry metadata travels as headers instead ({@code domain.model.RetryHeaderNames}).
 */
public record WebhookDeliveryRequested(
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        UUID causationEventId,
        String traceId,
        String correlationId) {}
