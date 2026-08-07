# ADR-0005: Database per service, no shared schemas

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** all persistent services; M6 (outbox/Debezium), M13 (audit sink), M18

## Context

A shared database is the cheapest possible integration and the most expensive possible
coupling: every service's migrations become everyone's migrations, a slow query in one service
degrades all of them, and the "you own your data" boundary that makes events meaningful stops
existing. It also makes the outbox pattern pointless — if two services share a database, the
dual-write problem M6 exists to solve never appears in a recognisable form.

ADR-0004 already forbids synchronous reads across services; the database is the remaining back
door.

## Decision

Each service owns its persistent state exclusively. Ownership means: only that service's
process opens a connection to it, only that service's migrations touch it, and its schema is
an internal implementation detail that MAY change without notice.

| Service | Store | Reason |
|---|---|---|
| payment-api | PostgreSQL | Outbox requires the event write and the state write in one ACID transaction |
| ledger | PostgreSQL | Balance integrity, row-level locking, `SERIALIZABLE`-capable |
| psp-connector | PostgreSQL | Dedup table `(paymentId, providerEventId)` with a unique constraint (M5) |
| webhook-notifier | MongoDB | Delivery-attempt documents with heterogeneous response bodies; TTL indexes |
| analytics | MongoDB + RocksDB | Read projections + Kafka Streams state stores |
| audit-trail | MongoDB | Derived, write-only, populated by a Kafka Connect sink (M13) — owned by no service |
| realtime-gateway, api-gateway, discovery-server | none | Stateless by design |

Rules:
- **No cross-service foreign keys, joins, or views.** A `merchantId` in the ledger is an
  opaque identifier, not a reference.
- **No shared migration project.** Each service runs its own Flyway/Mongock.
- **No shared JPA entity classes.** `libs/common-events` carries events only; it MUST NOT
  contain `@Entity`, `@Document`, or repository types.
- **Physical vs logical separation.** In `docker-compose` one Postgres container and one
  MongoDB container host separate logical databases with separate users and separate
  passwords, each user granted only its own database. On Kubernetes (M18) each service gets
  its own StatefulSet. The compose shortcut is a resource concession, never a licence to join
  across databases — the per-user grant makes a cross-database query fail loudly.
- **Debezium** (M6) connects only to payment-api's database and is configured to capture only
  the `outbox` table. It is an extension of payment-api's write path, not a second reader of
  its business tables.
- **audit-trail** is derived state: it may be dropped and rebuilt from Kafka at any time and
  is never read by a service, only by humans and the UI.

## Consequences

**Positive**
- Distributed transactions are impossible, which forces the outbox (M6) and the saga (M11) —
  both of which are the point of the exercise.
- A schema change is a single-service deploy.
- Each service picks the store that fits its access pattern, which is why this system has both
  Postgres and MongoDB at all.

**Negative / accepted costs**
- **No global query.** "All payments for merchant X with their ledger entries" cannot be
  answered by SQL; it is answered by the analytics projection, which is eventually consistent
  and may briefly disagree with the ledger. The UI must show data as of a timestamp.
- Reference data (merchant config) is duplicated into every service (ADR-0004).
- More containers, more connection pools, more backup targets. At learning scale this shows up
  as laptop RAM.
- Cross-service reporting requires a dedicated pipeline. Here, that is the Connect sink into
  audit-trail — which is exactly why M13 includes it.

**Follow-ups**
- M2: compose provisions per-service users/databases with explicit grants.
- M18: per-service StatefulSets, secrets per service.

## Alternatives considered

**One database, schema per service.** Cheap and common. Rejected: nothing prevents a join, and
in practice someone always writes one. Also removes the dual-write problem from M6.

**One database, one schema.** Rejected outright — it is a distributed monolith.

**Postgres everywhere, no MongoDB.** Tempting for operational simplicity, and `jsonb` would
cover the webhook attempt documents. Rejected because the plan deliberately wants a
document-store consumer and a Connect sink target; the polyglot choice is pedagogical, and
this ADR should say so honestly rather than pretend it is a performance decision.
