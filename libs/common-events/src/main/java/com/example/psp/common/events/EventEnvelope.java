package com.example.psp.common.events;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Shared event envelope - ADR-0002.
 *
 * <p>Every event topic carries this envelope embedded as a named sub-record alongside the
 * domain fields at the top level of the concrete event (e.g. {@code PaymentRequested { envelope,
 * paymentId, merchantId, amount, ... } }). There is deliberately <b>no generic {@code payload}
 * field</b> here: a generic payload would force Schema Registry into either an opaque
 * {@code bytes} type or a union of every event type, defeating per-topic schema validation
 * (see ADR-0002, "Alternatives considered").
 *
 * <p>This is the JSON-era shape. Avro codegen for the same fields arrives in M9; until then,
 * concrete per-service event types compose this record directly (e.g. as a Jackson-serialized
 * Java record) and this module stays a plain Java library with no Spring or Avro dependency
 * (ADR-0007).
 *
 * @param eventId       UUIDv7, producer-generated. <b>The</b> idempotency key for consumer
 *                      dedup (M5). Use {@link UuidV7#generate()} to create one.
 * @param eventType     discriminator, e.g. {@code "payments.payment-requested.v1"} (ADR-0001).
 * @param eventVersion  schema/event version for this {@code eventType}; starts at 1.
 * @param aggregateId   the entity the event is about ({@code paymentId}, {@code refundId},
 *                      {@code merchantId}). The UI grouping key; usually not the partition key
 *                      (ADR-0003).
 * @param aggregateType the type of the aggregate identified by {@code aggregateId}, e.g.
 *                      {@code "payment"}.
 * @param occurredAt    domain event time - when the fact happened, not when it was published.
 *                      Streams {@code TimestampExtractor} reads this field (M10).
 * @param source        the producing service name, for provenance and DLQ triage.
 * @param traceId       W3C trace-id, propagated end to end (M15).
 * @param correlationId the originating request id, assigned at the gateway.
 * @param causationId   the {@code eventId} of the event that caused this one, or {@code null}
 *                      for a root event. The causation chain gives the refund-tracker UI (M17)
 *                      its saga graph for free.
 */
public record EventEnvelope(
        UUID eventId,
        String eventType,
        int eventVersion,
        String aggregateId,
        String aggregateType,
        Instant occurredAt,
        String source,
        String traceId,
        String correlationId,
        UUID causationId) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId must not be null");
        requireNonBlank(eventType, "eventType");
        if (eventVersion < 1) {
            throw new IllegalArgumentException("eventVersion must be >= 1, was " + eventVersion);
        }
        requireNonBlank(aggregateId, "aggregateId");
        requireNonBlank(aggregateType, "aggregateType");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        requireNonBlank(source, "source");
        requireNonBlank(traceId, "traceId");
        requireNonBlank(correlationId, "correlationId");
        // causationId is intentionally nullable: null marks a root event with no cause.
    }

    /**
     * Creates a root envelope (no {@code causationId}) with a fresh UUIDv7 {@code eventId} and
     * {@code occurredAt} set to now.
     */
    public static EventEnvelope root(
            String eventType,
            int eventVersion,
            String aggregateId,
            String aggregateType,
            String source,
            String traceId,
            String correlationId) {
        return new EventEnvelope(
                UuidV7.generate(),
                eventType,
                eventVersion,
                aggregateId,
                aggregateType,
                Instant.now(),
                source,
                traceId,
                correlationId,
                null);
    }

    /**
     * Like {@link #causedBy(UUID, String, int, String, String, String, String, String)} but with a
     * caller-supplied {@code eventId} instead of a freshly minted one. {@code eventId} is the
     * consumer-side idempotency key (see {@link #eventId()}), so a publisher that may have to
     * re-emit the same logical event after a redelivery - psp-connector republishing a status
     * event whose attempt row already exists (M19 drill 9's loss) - must carry the id the
     * original publish carried, or downstream dedup cannot recognise the replay and books it
     * twice. {@code occurredAt} is still now(): identity is the id, not the wall clock.
     */
    public static EventEnvelope causedBy(
            UUID eventId,
            UUID causeEventId,
            String eventType,
            int eventVersion,
            String aggregateId,
            String aggregateType,
            String source,
            String traceId,
            String correlationId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(causeEventId, "causeEventId must not be null");
        return new EventEnvelope(
                eventId,
                eventType,
                eventVersion,
                aggregateId,
                aggregateType,
                Instant.now(),
                source,
                traceId,
                correlationId,
                causeEventId);
    }

    /**
     * Creates an envelope caused by {@code causeEventId}, propagating {@code traceId} and
     * {@code correlationId} from the causing event - the usual case for every event after the
     * first in a saga.
     */
    public static EventEnvelope causedBy(
            UUID causeEventId,
            String eventType,
            int eventVersion,
            String aggregateId,
            String aggregateType,
            String source,
            String traceId,
            String correlationId) {
        Objects.requireNonNull(causeEventId, "causeEventId must not be null");
        return new EventEnvelope(
                UuidV7.generate(),
                eventType,
                eventVersion,
                aggregateId,
                aggregateType,
                Instant.now(),
                source,
                traceId,
                correlationId,
                causeEventId);
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
