# Kafka PSP Pipeline

A mini PSP (payment service provider) event pipeline, built module by module to reach an
intermediate Kafka level: Java/Spring Boot microservices, hexagonal (ports & adapters)
architecture, MapStruct + Lombok, a React + TypeScript UI, fully deployable to Kubernetes.

Full plan: [`docs/PLAN.md`](docs/PLAN.md). Decisions and their rationale:
[`docs/adr/`](docs/adr/). Diagrams: [`docs/diagrams/`](docs/diagrams/).
Measured failure drills against the live cluster:
[`docs/M19-failure-drills.md`](docs/M19-failure-drills.md) (part 1),
[`docs/M19-failure-drills-part2.md`](docs/M19-failure-drills-part2.md) (part 2).

## Overview

**Services:** `payment-api`, `psp-connector`, `ledger`, `webhook-notifier`, `analytics`,
`realtime-gateway`, `api-gateway`, `discovery-server`, plus a React UI.

**Flow:** `POST /payments` → `payment-api` (Postgres + outbox) → `payments.requested` →
`psp-connector` (simulated provider) → `payments.status-changed` → `ledger` (EOS balances) +
`webhook-notifier` (retries/DLQ) + `analytics` (Streams windows) + `realtime-gateway` (SSE to UI).

Every inter-service communication is a Kafka event; commands enter once, via REST, through the
gateway (ADR-0004). Database per service, no shared schemas (ADR-0005).

## Quickstart

Prerequisites: **Java 21** (SDKMAN) and **Maven**.

```bash
java --version
mvn --version
```

Build everything (parent POM, both libs, `payment-api`), running the ArchUnit hexagon check:

```bash
mvn clean verify
```

Run `payment-api` standalone - no database, no Kafka broker, no Docker required at M1:

```bash
mvn -pl services/payment-api -am spring-boot:run
```

```bash
curl -i -X POST http://localhost:8081/api/payments \
  -H 'Content-Type: application/json' \
  -d '{"merchantId":"merchant-1","amount":10.00,"currency":"EUR"}'

curl -i http://localhost:8081/actuator/health
```

`payment-api`'s persistence and event-publishing adapters are in-memory/logging stubs at this
stage (see [Module index](#module-index-a-learning-path)) - real PostgreSQL and Kafka wiring
land in M2/M3.

## Module index (a learning path)

Each module builds on the last; work through them in order. Full detail, "prove it" experiments,
and time estimates live in [`docs/PLAN.md`](docs/PLAN.md).

| Module | Path(s) | Teaches | Status |
|---|---|---|---|
| M1 - Repo scaffold | `libs/common-events`, `libs/common-web`, `services/payment-api` | Maven multi-module, Lombok/MapStruct wiring, hexagonal layout, ArchUnit enforcement | **Done** |
| M2 - Infra baseline | `infra/compose/` | KRaft, `advertised.listeners`, broker vs topic config precedence | **Done** |
| M3 - First producer | `services/payment-api` | `ProducerRecord`, acks, idempotence, batching, key choice | **Done** |
| M4 - First consumer | `services/psp-connector` | Consumer groups, poll loop, manual ack, rebalancing | **Done** |
| M5 - Idempotency & dedup | `services/psp-connector` | At-least-once + idempotent consumer dedup | **Done** |
| M6 - Transactional outbox | `services/payment-api` | Dual-write problem, outbox, Debezium/Connect | **Done** |
| M7 - Ledger: exactly-once | `services/ledger` | Kafka transactions, EOS boundary, zombie fencing | **Done** |
| M8 - Retries, DLQ | `services/webhook-notifier` | Non-blocking retry topics, `DefaultErrorHandler`, DLQ | **Done** |
| M9 - Schemas & evolution | `libs/common-events` (Avro) | Schema Registry, compatibility modes | **Done** |
| M10 - Compacted topics & Streams | `services/analytics` | `KTable`/`GlobalKTable`, RocksDB, windows | **Done** |
| M11 - Refund saga | `services/ledger`, `services/psp-connector` | Choreography, compensating transactions | **Done** |
| M12 - Request-reply + realtime-gateway | `services/realtime-gateway` | `ReplyingKafkaTemplate`, the broadcast problem | **Done** |
| M13 - Feature grab-bag | multiple | Streams joins, batch listener, claim check, quotas, Connect sink | **Done** - 5/5, all measured (claim check: `services/payment-api/README.md`'s "M13 - Claim check, measured") |
| M14 - Security | all services | SASL/SCRAM, ACLs, TLS | **Done** |
| M15 - Observability | all services | Trace propagation via Kafka headers, lag dashboards | **Done** - metrics on both stacks (compose, and on k8s via Strimzi `metricsConfig`/`kafkaExporter` + Prometheus/Grafana in `monitoring`; tracing still compose-only) |
| M16 - Discovery + gateway | `services/discovery-server`, `services/api-gateway` | Eureka vs k8s-native discovery, Spring Cloud Gateway | **Done** |
| M17 - React UI | `ui/` | SSE, OpenAPI-generated types, the six showcase pages | **Done** |
| M18 - Kubernetes | `infra/k8s/` | Strimzi, Helm, KEDA | **Done** |
| M19 - Failure drills | [`docs/M19-failure-drills.md`](docs/M19-failure-drills.md), [`part 2`](docs/M19-failure-drills-part2.md) | ISR/acks trade-offs, unclean leader election, partition count vs keyed ordering, rebalance cost, static membership, throttled reassignment, retention semantics, timestamp offset reset, chaos (a false alarm that surfaced a real latent loss window, fixed + regression-proven), Testcontainers ITs (`mvn verify`) asserting no loss across a real rebalance, EOS + dedup, the retry chain, and outbox atomicity | **Done** |

## Stack & persistence decisions

| Service | DB | Why |
|---|---|---|
| payment-api | PostgreSQL | outbox needs ACID |
| ledger | PostgreSQL | balance integrity, transactions |
| psp-connector | PostgreSQL | dedup table with unique constraints |
| webhook-notifier | MongoDB | delivery-attempt documents, TTL indexes |
| analytics | MongoDB + RocksDB | read projections + Streams state |
| audit-trail | MongoDB | written by a Kafka Connect **sink**, zero code |

## Lombok + MapStruct rules

- Add `lombok-mapstruct-binding` and order annotation processors correctly - the #1 setup gotcha
- No `@Data` on JPA entities (equals/hashCode + lazy-loading pitfalls); use `@Getter/@Setter/@Builder`
- Java records for DTOs and events; Lombok only where records don't fit
- MapStruct: `componentModel = "spring"`, `unmappedTargetPolicy = ERROR`, one mapper per hexagon boundary

The annotation-processor order is fixed once, in the root `pom.xml`'s `maven-compiler-plugin`
configuration, and inherited by every module: **Lombok → `lombok-mapstruct-binding` →
MapStruct**. Getting this order wrong produces MapStruct mappers that silently ignore
Lombok-generated accessors.

## Repo layout

```
kafka-psp-pipeline/
├── README.md
├── docs/{adr, diagrams}/
├── infra/
│   ├── compose/               # docker-compose profiles
│   └── k8s/                   # kind config, helm charts, strimzi, keda
├── libs/
│   ├── common-events/         # event envelope (ADR-0002), Avro schemas from M9
│   └── common-web/            # RFC-7807 error handling, correlation-id filter
├── services/
│   ├── payment-api/  psp-connector/  ledger/
│   ├── webhook-notifier/  analytics/
│   ├── realtime-gateway/
│   ├── api-gateway/  discovery-server/
└── ui/                         # React + TS (M17)
```

## Hexagonal template per service

```
domain/          # pure Java: aggregate, value objects, ports (interfaces). No Spring, no Kafka
application/     # use cases orchestrating ports
adapters/
  in/web         # controllers, DTOs, MapStruct dto<->domain
  in/kafka       # listeners, MapStruct avro<->domain
  out/persistence# entities, repos, MapStruct entity<->domain
  out/kafka      # producers
config/
```

The test that you're doing it right: `domain/` compiles with zero framework dependencies -
enforced per service by an ArchUnit test (see `services/payment-api/.../architecture/HexagonalArchitectureTest.java`).
