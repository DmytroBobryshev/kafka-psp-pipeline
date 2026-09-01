package com.example.psp.common.events;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

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
