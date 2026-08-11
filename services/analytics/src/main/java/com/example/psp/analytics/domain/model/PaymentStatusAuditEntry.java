package com.example.psp.analytics.domain.model;

import java.time.Instant;

/**
 * One raw {@code payments.payment-status-changed.v1} event, kept verbatim for audit purposes
 * (M13's batch listener). Pure Java (ADR-0007) - the {@code @KafkaListener(batch = true)} maps
 * the Avro record to this type at the inbound boundary, the same discipline
 * {@code adapters.in.kafka.AnalyticsTopology} follows for the Streams path.
 *
 * <p>Deliberately NOT the same thing as M10's {@code PaymentOutcome}: that type is an
 * aggregation input (enriched, ephemeral, never persisted on its own); this one is the audit
 * record itself; the persisted unit, one document per event, kept exactly as it was seen rather
 * than folded into anything.
 *
 * @param eventId    {@code envelope.eventId} - THE idempotency key (ADR-0002), and this
 *                   collection's Mongo {@code _id}. A payment status change is reported exactly
 *                   once by psp-connector per {@code providerReference}, so this is a genuine
 *                   natural key, not a synthetic one.
 * @param paymentId  the payment this status change is about.
 * @param merchantId the owning merchant; also the topic's partition key (ADR-0003).
 * @param status     {@code SUCCEEDED} or {@code DECLINED}.
 * @param occurredAt {@code envelope.occurredAt} - domain event time, not ingest time.
 */
public record PaymentStatusAuditEntry(
        String eventId, String paymentId, String merchantId, String status, Instant occurredAt) {}
