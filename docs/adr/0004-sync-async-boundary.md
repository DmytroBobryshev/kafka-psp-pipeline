# ADR-0004: REST at the edge only; all inter-service communication over Kafka

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** every service; M12, M16 in particular

## Context

The default microservice reflex is REST between services. It is easy to start with and it
quietly reintroduces the monolith's coupling: a synchronous call chain fails as a unit, its
latency is the sum of its parts, and the caller must know the callee exists. This project's
purpose is to learn Kafka, and a system that reaches for REST between services never
encounters the problems Kafka exists to solve.

## Decision

**Inbound.** All external traffic — the React UI and merchant API clients — enters through
`api-gateway` (Spring Cloud Gateway) over HTTPS. The gateway is the only component with a
public route. It owns CORS, rate limiting, the circuit breaker, and correlation-id injection
(M16).

**Between services.** Zero service-to-service REST, RSocket, or gRPC. Every cross-service
interaction is a Kafka record. A service that needs another service's data consumes the
relevant events and maintains its own local read model (CQRS): analytics keeps merchant
projections in MongoDB, every service holds `merchants.merchant-config-changed.v1` in a
`GlobalKTable` (M10) instead of calling a merchant service.

**Commands vs events.** A REST request is turned into an event by the service that owns the
aggregate, inside the same database transaction as the state change, via the outbox (M6).
Services do not publish events about aggregates they do not own.

**Synchronous need → Kafka request-reply.** When a caller genuinely must block for an answer
(the provider status check in M12), use `ReplyingKafkaTemplate` with a correlation-id header
and a dedicated reply topic, not an HTTP call. This is deliberately more awkward than REST;
the awkwardness *is* the lesson about when async is the wrong tool.

**Push to the browser.** `realtime-gateway` consumes `payments.*` / `refunds.*` and pushes SSE
to the UI. It never queries another service.

**Explicit carve-outs** (these are not service-to-service calls):
1. Outbound HTTP that **leaves** the system — merchant webhook callbacks (webhook-notifier),
   the simulated acquirer (psp-connector).
2. Infrastructure endpoints — Schema Registry, Actuator/health, Eureka (ADR-0009), Prometheus
   scraping, Kafka Connect REST API.
3. `api-gateway` → service HTTP, which is edge-to-service, not service-to-service.

**Enforcement.** An ArchUnit test in each service asserts that no `RestTemplate`,
`WebClient`, or `@FeignClient` type is referenced outside packages annotated as external
integrations (`adapters.out.http`), and that `adapters.out.http` exists only in psp-connector
and webhook-notifier.

## Consequences

**Positive**
- No synchronous call chains: a service being down produces consumer lag, not a cascade of
  5xx. This is what makes the M19 chaos drills meaningful.
- Adding a consumer to an existing event is a deploy of one service and no change anywhere
  else — realtime-gateway and analytics were both added this way.
- Backpressure is explicit and measurable (consumer lag), so KEDA can scale on it (M18).

**Negative / accepted costs**
- **Every read model is eventually consistent.** `POST /payments` returns `202 Accepted` with
  a `paymentId`, not a final status; the UI must be built around the live timeline rather than
  a request/response result. This shapes M17 page 1.
- Data is duplicated across services by design. Merchant config lives in five processes.
- Debugging spans processes. This is why M15 (trace context in headers) is not optional in
  practice, even though the plan lists it as droppable.
- The request-reply path (M12) reimplements, badly, what HTTP gives for free. Accepted for its
  teaching value; it is used exactly once.

## Alternatives considered

**REST for queries, Kafka for commands/events.** The common pragmatic compromise, and probably
what a real team should do. Rejected here: it removes the pressure to build local read models
and `GlobalKTable` lookups, which is most of M10.

**gRPC between services.** Same coupling as REST with better ergonomics and schemas. Rejected
for the same reason.

**Service mesh with retries/circuit breaking (Istio, Linkerd).** Addresses the failure-cascade
symptom of synchronous calls without addressing the coupling. Adds a large operational surface
to a learning cluster. Rejected.
