package com.example.psp.realtimegateway.domain.model;

import java.time.Instant;

/**
 * A flattened, transport-agnostic view of one domain event this gateway pushes to browsers.
 * Pure Java, no framework dependency (ADR-0007) - built by
 * {@code adapters.in.kafka.RealtimeEventMapper} from whichever of the 7 generated Avro classes
 * this gateway consumes ({@code payments.payment-requested.v1}, {@code
 * payments.payment-status-changed.v1}, every {@code refunds.*.v1} topic), so {@code domain/} and
 * {@code application/} never import Avro or Kafka (same rule every other hexagon boundary in this
 * codebase enforces).
 *
 * <p>Every one of those 7 Avro schemas carries {@code envelope}, {@code paymentId} AND
 * {@code merchantId} (verified against {@code libs/common-events/src/main/avro}) - which is
 * exactly what makes filtering by "paymentId and/or merchantId" (the module brief) work uniformly
 * across payment AND refund events with one {@link SubscriptionFilter} shape. {@code refundId},
 * {@code status}, {@code reason} and {@code providerReference} are nullable because not every
 * event type carries all of them - see the mapper for exactly which fields each event type fills
 * in.
 *
 * <p>IDs are kept as {@code String}, not {@code UUID}/typed values: this is a display/relay
 * service with no DLQ (docs/diagrams/topic-map.md: "analytics and realtime-gateway deliberately
 * have none: they log, count, and skip") - a malformed or unexpected id string should never throw
 * out of the mapper and poison-pill this consumer, so nothing here is validated beyond
 * non-nullability of the fields every schema guarantees.
 */
public record RealtimeEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        // causationId is the eventId of the event that caused this one (ADR-0002). Forwarding it
        // is what lets a client draw the causal chain rather than list events in arrival order -
        // the UI timeline renders "caused by <eventId>" and links the two.
        String causationId,
        // Which service emitted this. Without it a timeline shows what happened but not who did
        // it, which is most of what makes an event-driven system legible from the outside.
        String source,
        String paymentId,
        String merchantId,
        String refundId,
        String status,
        String reason,
        String providerReference) {
}
