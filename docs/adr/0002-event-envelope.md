# ADR-0002: Event envelope — custom envelope, CloudEvents-aligned

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** `libs/common-events`, every producer and consumer, M9 (Avro), M15 (tracing)

## Context

Every event needs identity (for the M5 dedup table), aggregate identity (for correlation and
UI grouping), a type discriminator, an occurrence timestamp (Streams windowing in M10 needs
event time, not ingest time), and a trace context (M15 traces one payment across services).

The candidate standard is **CloudEvents 1.0**, which specifies `id`, `source`, `specversion`,
`type`, `subject`, `time`, `datacontenttype`, and a `data` payload, with a Kafka binding in
both *binary* mode (attributes → headers, `data` → value) and *structured* mode (everything in
the value).

The hard constraint is Confluent Schema Registry with `TopicNameStrategy` (ADR-0001): the
registry validates **one concrete schema per topic-value subject**. A generic
`data`/`payload` field forces either `bytes` (schema-opaque — the registry can no longer
validate or evolve the payload, and AKHQ shows binary) or a union of every payload type (which
must be extended for every new event, i.e. a global breaking change).

## Decision

Use a **custom envelope, field-for-field mappable to CloudEvents**, embedded as a named record
rather than wrapping an opaque payload.

Each event is one flat Avro record per topic. It carries an `envelope` sub-record and the
domain fields at the top level — there is **no generic `payload` field**:

```
EventEnvelope { eventId, eventType, eventVersion, aggregateId, aggregateType,
                occurredAt, source, traceId, correlationId, causationId }
PaymentRequested { envelope, paymentId, merchantId, amount, currency, method, ... }
```

Rules:
- `eventId` — UUID v7, producer-generated, **the** idempotency key for consumer dedup (M5).
- `aggregateId` — the entity the event is about (`paymentId`, `refundId`, `merchantId`). It is
  the UI grouping key and, in most cases, not the partition key (see ADR-0003).
- `occurredAt` — `timestamp-millis`, **domain event time** (when the fact happened), not
  publish time. Streams `TimestampExtractor` reads it.
- `traceId` — W3C trace-id; `correlationId` — the originating request id from the gateway;
  `causationId` — the `eventId` that caused this one. The chain `causationId` gives the M17
  refund tracker its saga graph for free.
- `source` — the producing service name, for provenance and DLQ triage.

**Header duplication.** The producer interceptor also writes `traceparent`, `event-id`,
`event-type`, and `aggregate-id` as Kafka headers. Headers are for infrastructure that must
route, filter, or trace **without deserializing** (Micrometer/OTel propagation in M15, DLQ
triage, AKHQ filtering). The value stays the single source of truth; on disagreement, the
value wins.

**CloudEvents mapping** (kept in `libs/common-events` so a future binding is mechanical):
`eventId`→`id`, `eventType`→`type`, `occurredAt`→`time`, `aggregateId`→`subject`,
`source`→`source`, `traceId`→`traceparent` (distributed-tracing extension), `correlationId`
and `causationId` → CloudEvents extensions.

## Consequences

**Positive**
- Schema Registry validates and evolves the **whole** event, payload included; `BACKWARD`
  compatibility checks are real (M9 depends on this).
- Generated Avro classes are strongly typed end to end — no cast, no second deserialization
  step, no `JsonNode`.
- Envelope evolution is one shared record: adding `causationId` later would have been a single
  compatible change across all topics.

**Negative / accepted costs**
- Not wire-compatible with CloudEvents tooling (Knative, EventBridge, Dapr). If interop is
  ever needed, a Connect SMT does the conversion at the boundary — the mapping table above is
  the spec for it.
- The shared `EventEnvelope` record is a coupling point: a breaking change to it breaks every
  topic at once. Mitigation: envelope changes MUST be additive-with-default, forever.
- Headers duplicate four fields, costing a few dozen bytes per record and creating a
  divergence risk if a producer sets one and not the other. Mitigation: only the shared
  interceptor writes them; services never set them by hand.

## Alternatives considered

**CloudEvents structured mode + Avro.** The `data` field becomes `bytes` or a union. Loses
per-topic schema validation, which is the single thing M9 exists to teach. Rejected.

**CloudEvents binary mode (attributes in headers, domain data in the value).** This is the
closest call — it keeps the value cleanly typed and is genuinely standard. Rejected because
headers are not covered by Schema Registry: a missing or malformed attribute is discovered at
runtime, not at produce time, and event-time windowing (M10) would depend on an unvalidated
header. Envelope-in-value makes the contract enforceable.

**Envelope in headers only, no envelope in value.** Same objection, plus the record becomes
un-self-describing once it lands in the MongoDB audit sink (M13) or a DLQ dump.

**JSON with a `payload: object` field, no registry.** Fine for M3–M8, and it is in fact what
M3 ships. But M9 migrates to Avro, and this ADR is written so that migration is a serializer
swap rather than a redesign.
