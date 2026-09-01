# Kafka PSP Pipeline - Comprehensive Learning Plan

**Goal:** reach intermediate Kafka level by building a mini PSP event pipeline - Java/Spring Boot microservices, React + TypeScript UI, API gateway, discovery server, PostgreSQL/MongoDB, MapStruct + Lombok, SOLID + clean (hexagonal) architecture, fully deployable to Kubernetes.

**Effort:** ~3.5-4.5 months at a few evenings per week.

## System at a glance

Services: `payment-api`, `psp-connector`, `ledger`, `webhook-notifier`, `analytics`, `realtime-gateway`, `api-gateway`, `discovery-server` + React UI.

Flow: `POST /payments` -> payment-api (Postgres + outbox) -> `payments.requested` -> psp-connector (simulated provider) -> `payments.status-changed` -> ledger (EOS balances) + webhook-notifier (retries/DLQ) + analytics (Streams windows) + realtime-gateway (SSE to UI).

---

## Phase 0 - Research & Design (~1 week, before any code)

### 0.1 Best practices investigation -> deliverable: `docs/adr/`

Write a short ADR (Architecture Decision Record) per decision - writing them *is* the investigation:

- **Topic naming:** `payments.payment-requested.v1` style; version-suffix strategy for breaking schema changes
- **Event envelope:** `eventId, aggregateId, type, occurredAt, traceId, payload` - compare with CloudEvents spec, pick one
- **Keys and partition counts** per topic - document which ordering guarantee each key buys and the hot-partition risk
- **Sync/async rule:** commands enter via REST through the gateway; *all* inter-service communication is Kafka events; no service-to-service REST
- **Database per service**, no shared schemas
- **Error taxonomy:** retryable vs non-retryable exceptions - drives the retry/DLQ policy (M8)
- **Monorepo**, Maven multi-module, package-by-hexagon inside each service

### 0.2 Microservice design -> deliverable: `docs/diagrams/` (Mermaid)

- C4 context + container diagrams
- **Topic map table:** name, key, partitions, retention, cleanup.policy, producers, consumers - the single most useful design artifact
- Sequence diagrams: happy-path payment, refund saga, DLQ replay
- **Saga style:** choreography (recommended - it exercises Kafka; orchestration centralizes logic in one service, less Kafka learning)
- **Discovery decision:** Eureka is redundant on Kubernetes, where DNS + Services *are* the discovery mechanism. Best-case design: Eureka active only in the `docker-compose` Spring profile, native k8s discovery in the `k8s` profile. You learn both and understand why platforms replaced Eureka.

### 0.3 Toolchain installation

- Docker Desktop / Rancher Desktop
- **kind** + kubectl + helm + k9s (kind over minikube: faster, config-as-code, multi-node; create the cluster from `infra/k8s/kind.yaml` with ingress port mappings)
- Node 20+, pnpm
- **Done when:** `kubectl get nodes` shows the cluster and a hello-world deployment serves through ingress

---

## Repo layout

```
kafka-psp-pipeline/
├── README.md                  # main: overview, C4 diagram, quickstart, module index
├── docs/{adr, diagrams}/
├── infra/
│   ├── compose/               # docker-compose profiles
│   └── k8s/                   # kind config, helm charts, strimzi, keda
├── libs/
│   ├── common-events/         # envelope, Avro schemas, generated classes
│   └── common-web/            # error handling, correlation-id filter
├── services/
│   ├── payment-api/  psp-connector/  ledger/
│   ├── webhook-notifier/  analytics/
│   ├── realtime-gateway/      # SSE push to UI
│   ├── api-gateway/  discovery-server/
└── ui/                        # React + TS
```

## Stack & persistence decisions

| Service | DB | Why |
|---|---|---|
| payment-api | PostgreSQL | outbox needs ACID |
| ledger | PostgreSQL | balance integrity, transactions |
| psp-connector | PostgreSQL | dedup table with unique constraints |
| webhook-notifier | MongoDB | delivery-attempt documents, TTL indexes |
| analytics | MongoDB + RocksDB | read projections + Streams state |
| audit-trail | MongoDB | written by a Kafka Connect **sink**, zero code |

### Lombok + MapStruct rules (put in root README)

- Add `lombok-mapstruct-binding` and order annotation processors correctly - the #1 setup gotcha
- No `@Data` on JPA entities (equals/hashCode + lazy-loading pitfalls); use `@Getter/@Setter/@Builder`
- Java records for DTOs and events; Lombok only where records don't fit
- MapStruct: `componentModel = "spring"`, `unmappedTargetPolicy = ERROR`, one mapper per hexagon boundary

### Hexagonal template per service (SOLID enforcement)

```
domain/          # pure Java: Payment, Money, ports (interfaces). No Spring, no Kafka
application/     # use cases orchestrating ports
adapters/
  in/web         # controllers, DTOs, MapStruct dto<->domain
  in/kafka       # listeners, MapStruct avro<->domain
  out/persistence# entities, repos, MapStruct entity<->domain
  out/kafka      # producers
config/
```

The test that you're doing it right: `domain/` compiles with zero framework dependencies.

---

## Phase 1 - Foundations

### M1 - Repo scaffold (4-6 h)

Parent POM, `common-events`, `common-web` (error handling, correlation-id filter), hexagonal archetype service, Lombok/MapStruct wiring, root README skeleton, ADR template.

### M2 - Infrastructure baseline (2-4 h)

**Build:** Docker Compose - 3 Kafka brokers in KRaft mode, Schema Registry, AKHQ (or Redpanda Console), PostgreSQL, MongoDB, Prometheus + Grafana.

**Learn:** KRaft vs ZooKeeper, controller quorum, `advertised.listeners` (the #1 misconfiguration), broker vs topic-level config precedence.

**Prove it:** create a topic with `--partitions 6 --replication-factor 3`, run `kafka-topics --describe`, explain leader vs ISR for each partition out loud.

**Done when:** `docker stop` on one broker and the cluster still serves reads/writes.

---

## Phase 2 - Core pipeline

### M3 - payment-api: first producer (4-6 h)

**Build:** `POST /payments` -> validate -> Postgres insert -> publish `payments.requested`. JSON for now (Avro comes in M9). Hexagonal layout; MapStruct at all three boundaries.

**Learn:** `ProducerRecord`, key vs value, `acks`, `retries`, `enable.idempotence`, `max.in.flight.requests.per.connection`, batching (`linger.ms`, `batch.size`), sync vs async send, `KafkaTemplate` callbacks.

**Key decision:** key by `merchantId` or `paymentId`? Write down the ordering guarantee and hot-partition risk of each - this choice ripples through every later module.

**Prove it:** produce 10k records with `acks=0`, `acks=1`, `acks=all` and compare throughput; kill the partition leader mid-run under `acks=1` and count what got lost.

### M4 - psp-connector: first consumer (4-6 h)

**Build:** consume `payments.requested`, simulate a provider (100ms-5s latency, 10% declines, 5% timeouts), publish `payments.status-changed`.

**Learn:** consumer groups, `group.id`, `auto.offset.reset`, the poll loop, `enable.auto.commit=false` + manual ack, Spring `AckMode`, `max.poll.records`, `max.poll.interval.ms`, `session.timeout.ms` vs `heartbeat.interval.ms`.

**Prove it:**
- Set `max.poll.interval.ms=5000` with 10s processing - watch the rebalance storm, understand exactly why it happens
- 6 consumer instances on a 6-partition topic, then add a 7th - explain why it sits idle
- Kill an instance mid-batch with auto-commit on - count duplicates vs lost messages

### M5 - Idempotency & duplicate handling (6-8 h)

**Build:** psp-connector emits duplicate callbacks on purpose. Consumers dedup via a Postgres table keyed `(paymentId, providerEventId)` with a unique constraint (or Redis + TTL).

**Learn:** at-least-once is the default and duplicates *will* happen; idempotent consumer patterns; producer `enable.idempotence` only dedups within a producer session, not end-to-end.

**Prove it:** reset offsets to earliest and replay the whole topic - balances must come out identical. If not, the idempotency is fake.

The single most valuable module for payments work.

### M6 - Transactional outbox (6-8 h)

**Build:** replace write-then-publish with an `outbox` table in the same DB transaction; polling publisher or Debezium CDC -> Kafka Connect.

**Learn:** the dual-write problem, outbox pattern, Connect architecture (workers, connectors, tasks, converters), Debezium change events, SMTs to route by aggregate type.

**Prove it:** kill payment-api between the DB commit and the publish - the event must still arrive.

Note: Connect gets re-deployed via Strimzi CRs in M18.

### M7 - ledger: exactly-once (8-10 h)

**Build:** consume `payments.status-changed`, maintain merchant balances in Postgres, publish `ledger.entries`.

**Learn:** Kafka transactions, `transactional.id`, `initTransactions`/`beginTransaction`/`sendOffsetsToTransaction`/`commit`, `isolation.level=read_committed`, `@Transactional` with `KafkaTransactionManager`, transaction coordinator and markers, zombie fencing via epochs.

**Understand the limit:** Kafka EOS covers Kafka-to-Kafka only, *not* Kafka-to-Postgres - for that you're back to M5 idempotency. Articulating this distinction is a genuine intermediate marker.

**Prove it:** kill the ledger mid-transaction; a `read_committed` consumer sees no partial writes, a `read_uncommitted` consumer *does* see aborted records.

### M8 - Retries, DLQ, poison pills (6-8 h)

**Build:** webhook-notifier delivering merchant HTTP callbacks; failures route `webhooks.retry.5s` -> `retry.1m` -> `retry.15m` -> `webhooks.dlq`; DLQ replay endpoint; delivery-attempt log in MongoDB (TTL indexes).

**Learn:** why blocking retries (`Thread.sleep` in the listener) destroy the consumer group - they blow `max.poll.interval.ms` and stall the whole partition; non-blocking retry topics, `@RetryableTopic`, `DefaultErrorHandler`, `ErrorHandlingDeserializer`, `BackOff` strategies, retry headers.

**Prove it:** publish a record whose bytes can't be deserialized; watch the consumer loop forever on one offset without `ErrorHandlingDeserializer`, then fix it.

The DLQ gets a UI console in M17.

### M9 - Schemas & evolution (6-8 h)

**Build:** migrate all topics from JSON to Avro (or Protobuf) with Schema Registry; schemas live in `libs/common-events`. Then evolve: add an optional field, add a required field, remove a field, rename a field.

**Learn:** wire format (magic byte + schema ID), `BACKWARD`/`FORWARD`/`FULL`/`NONE` compatibility, `specific.avro.reader`, Avro Maven plugin codegen, independent producer/consumer deploys, subject naming strategies.

**Prove it:** deploy a v2 producer against a running v1 consumer; then break compatibility deliberately and read the registry's rejection message until it makes sense.

### M10 - Compacted topics & Kafka Streams (10-12 h)

**Build:**
- `merchants.config` as a compacted topic; services load it into a `GlobalKTable`; tombstones for deletion
- analytics: volume, decline rate, avg latency per merchant in 1-minute tumbling windows; interactive queries; projections persisted to MongoDB

**Learn:** `cleanup.policy=compact` vs `delete`, log segments, `min.cleanable.dirty.ratio`, tombstones, `delete.retention.ms`; `KStream` vs `KTable` vs `GlobalKTable`, RocksDB state stores, changelog and repartition topics, tumbling/hopping/session windows, grace period, `num.stream.threads`, interactive queries.

**Prove it:** kill analytics and restart - state restores from the changelog without recomputation; send a tombstone and confirm the `GlobalKTable` lookup returns null.

---

## Phase 3 - Extended Kafka features

### M11 - Refund saga (choreography, 6-8 h)

**Build:** `refunds.requested` -> ledger reserves funds -> psp-connector executes -> success/failure events -> compensating event releases the reservation on failure.

**Learn:** compensating transactions, saga state tracking, eventual consistency you can *see* (visualized in the UI in M17).

### M12 - Request-reply + realtime-gateway (6-8 h)

**Build:**
- `ReplyingKafkaTemplate`, correlation-ID headers, reply topics - synchronous provider status check over Kafka
- realtime-gateway consumes `payments.*` and pushes SSE to browsers

**Learn:** the **broadcast problem** - every instance needs every event, so each instance uses a unique `group.id` (or partition assignment without a group). Consumer groups are for load-splitting, not fan-out - this module makes that click.

### M13 - Feature grab-bag (8-10 h)

- Streams **join**: `payments.requested` x `payments.status-changed` -> per-provider latency metric
- **Batch listener** in analytics (`@KafkaListener(batch = true)`)
- **Claim check**: dispute documents > 1MB -> MinIO, event carries a reference; touch `max.request.size`, `fetch.max.bytes`
- **Client quotas**: throttle a noisy producer, observe it in metrics
- **Kafka Connect sink**: MongoDB sink connector -> audit-trail collection, zero code

### M14 - Security (6-8 h)

SASL/SCRAM-SHA-512 per service principal, **ACLs** (payment-api may write `payments.requested` but not `ledger.entries` - prove it by trying), TLS encryption. Done before k8s so Strimzi `KafkaUser` CRs map onto it.

### M15 - Observability (4-6 h)

W3C trace context propagated through Kafka headers (Micrometer Tracing + OTel), Tempo/Jaeger - one payment traced across all services; kafka-lag-exporter dashboards in Grafana.

---

## Phase 4 - Platform & UI

### M16 - discovery-server + api-gateway (4-6 h)

- Eureka (docker-compose profile only, per ADR 0.2)
- Spring Cloud Gateway: routes, CORS for the UI, Redis rate limiting, Resilience4j circuit breaker, correlation-ID injection filter

### M17 - React + TypeScript UI (12-16 h)

**Stack:** Vite, TanStack Query + Router, `EventSource` (SSE), Tailwind + shadcn/ui, types generated from springdoc OpenAPI via `openapi-typescript`.

**Pages - each a window into a Kafka concept:**
1. **Create payment + live event timeline** - every event for a paymentId appears in real time via SSE. The showpiece.
2. **Merchant dashboard** - live windowed metrics from Streams interactive queries
3. **DLQ console** - browse failed messages, inspect retry headers and exception stack, replay button
4. **Merchant config editor** - writes through the API to the compacted topic; edit -> watch downstream services pick it up
5. **Cluster ops** - consumer lag, topic list via AdminClient
6. **Refund tracker** - saga steps and compensations visualized

---

## Phase 5 - Kubernetes

### M18 - k8s deployment module (10-14 h)

- **Strimzi operator**: `Kafka` CR (KRaft, 3 replicas), `KafkaTopic` CRs (topics as GitOps config - no more CLI), `KafkaUser` CRs (maps onto M14), `KafkaConnect`/`KafkaConnector` CRs for Debezium and the Mongo sink
- Helm umbrella chart + per-service charts; ConfigMaps/Secrets, liveness/readiness probes, resource limits
- **KEDA**: `ScaledObject` scaling psp-connector on consumer lag; load-test with k6 and watch pods scale out then in - the second showpiece
- nginx ingress for gateway + UI

**Prove it:** delete a broker pod (Strimzi heals it), roll a service under load with zero message loss.

### M19 - Operations & failure drills (8-10 h, on k8s)

- `min.insync.replicas=1` vs `=2` with `acks=all`, kill a broker under load, compare outcomes
- Unclean leader election on vs off - force it, observe divergence
- Add partitions to a live topic - explain why keyed ordering just broke
- `kafka-reassign-partitions` to move a partition across brokers
- Consumer lag dashboards in Grafana; deliberately induce lag, watch recovery — **done**, after a
  detour to put a metrics stack on the kind cluster first:
  [part 2, drill 10](M19-failure-drills-part2.md#drill-10---the-deferred-lag-drill-a-metrics-stack-on-kubernetes)
- `RangeAssignor` vs `CooperativeStickyAssignor` - measure rebalance stop-the-world time under both
- `group.instance.id` static membership - restart a pod and observe no rebalance
- Retention: `retention.ms` vs `retention.bytes`, segment rolling, `log.segment.bytes`
- Offset reset to a timestamp, not just earliest/latest
- Chaos: kill pods mid-transaction, cordon a node

**Plus:** Testcontainers integration tests for every module - a test asserting correct behaviour across a rebalance teaches more than any article. k6 for load generation.

---

## README convention

**Per module:** purpose + Kafka concepts demonstrated -> Mermaid architecture diagram -> topics table (name/key/partitions/retention) -> how to run (compose and k8s) -> "prove it" experiments -> troubleshooting.

**Root:** system overview, C4 diagram, quickstart, module index ordered as a learning path, stack decisions, Lombok/MapStruct rules.

---

## Sequencing & timeline

| Phase | Modules | Time |
|---|---|---|
| 0 Research & design | 0.1-0.3 | ~1 wk |
| 1 Foundations | M1-M2 | ~1 wk |
| 2 Core pipeline | M3-M10 | ~4-5 wks |
| 3 Extended features | M11-M15 | ~3-4 wks |
| 4 Platform & UI | M16-M17 | ~2-3 wks |
| 5 Kubernetes | M18-M19 | ~2-3 wks |

**Order exceptions allowed:** pull a minimal M17 (payment form + live timeline) forward right after M12 for motivation; keep M14 before M18 so security maps onto Strimzi.

**Cutting scope:** M13 and M15 are droppable without breaking the spine; M11, M12, M14, M18 are not - they carry the intermediate-level weight.

**Checkpoints:**
- After M5: explain without notes why at-least-once + idempotency is usually preferable to exactly-once in production payment systems.
- After M7: articulate exactly where Kafka EOS ends and idempotency takes over.
- After M19: "I've used Kafka" has become "I understand Kafka".
