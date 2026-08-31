package com.example.psp.webhooknotifier.adapters.out.kafka;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wire shape of {@code webhooks.webhook-delivery-requested.v2.dlq} ONLY, as this service reads
 * and writes it - the JSON, byte-tolerant record used for the terminal DLQ hop
 * ({@code KafkaWebhookDeliveryPublisher#send}'s dlq branch, {@code KafkaDlqReader}, and DLQ
 * replay's republish). Through M8 this same class also carried the base topic and all three retry
 * tiers; M9 Phase 2 moved those four to the generated Avro
 * {@code com.example.psp.common.events.avro.WebhookDeliveryRequested} record instead (used
 * directly by both {@code adapters.in.kafka} and {@code adapters.out.kafka} - a shared library
 * type, not a cross-adapter dependency, same precedent as {@code payments.payment-requested.v1}'s
 * Avro record in M9 Phase 1) and left ONLY the DLQ on this hand-written JSON shape, deliberately:
 * {@code DeadLetterPublishingRecoverer} must be able to republish the raw bytes of a genuine
 * poison pill, which {@code KafkaAvroSerializer} cannot do (see
 * {@code KafkaWebhookDeliveryPublisher}'s and {@code config.KafkaConsumerConfig}'s javadoc for the
 * full reasoning, and the README's M9 Phase 2 section for why the DLQ chain was cut to a NEW
 * {@code .v2.dlq} topic rather than reused in place).
 *
 * <p>No {@code EventEnvelope}: this is an internal command/work-item topic, not a business-event
 * one (ADR-0002), and the retry metadata travels as headers instead
 * ({@code domain.model.RetryHeaderNames}).
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
        String correlationId,
        String eventType,
        UUID refundId) {}
