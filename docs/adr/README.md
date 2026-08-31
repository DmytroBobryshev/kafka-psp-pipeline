# Architecture Decision Records

Long-lived decisions for the Kafka PSP pipeline. Written during Phase 0.1 — writing them *is*
the investigation. Everything downstream (topic map, service code, k8s manifests) must stay
consistent with what is recorded here; where an implementation module discovers that an ADR is
wrong, the fix is a new ADR that supersedes it, not an edit to the old one.

## Index

| # | Title | Status | Primary modules |
|---|---|---|---|
| [0000](0000-adr-template.md) | ADR template | — | — |
| [0001](0001-topic-naming-and-versioning.md) | Topic naming convention and version-suffix strategy | Accepted | M2, M9, M18 |
| [0002](0002-event-envelope.md) | Event envelope — custom envelope, CloudEvents-aligned | Accepted | M3, M9, M15 |
| [0003](0003-partition-keys-and-counts.md) | Partition keys and partition counts | Accepted | M3, M7, M10, M13, M19 |
| [0004](0004-sync-async-boundary.md) | REST at the edge only; all inter-service communication over Kafka | Accepted | M12, M16 |
| [0005](0005-database-per-service.md) | Database per service, no shared schemas | Accepted | M6, M13, M18 |
| [0006](0006-error-taxonomy-retry-dlq.md) | Error taxonomy — retryable vs non-retryable, retry/DLQ policy | Accepted | M4, M5, M8, M17 |
| [0007](0007-monorepo-maven-hexagonal.md) | Monorepo, Maven multi-module, package-by-hexagon | Accepted | M1, all |
| [0008](0008-saga-choreography.md) | Refund saga uses choreography, not orchestration | Accepted | M11, M17 |
| [0009](0009-service-discovery-per-profile.md) | Eureka in `compose`, native Kubernetes discovery in `k8s` | Accepted | M16, M18 |
| [0010](0010-kafka-security-model.md) | SASL/SCRAM per-service principals, deny-by-default ACLs | Accepted | M14, M17, M18 |

## How these fit together

- **0001 + 0002** define the wire contract: what a topic is called and what a record contains.
- **0003** is the load-bearing one — it fixes which orderings exist, and therefore what the
  ledger (M7), the saga (M11), and the Streams topology (M10) are allowed to assume.
- **0004 + 0005** together forbid every synchronous back channel (no REST between services, no
  shared database), which is what forces the outbox (M6) and the saga (M8/M11) to exist.
- **0006** turns failure into a classification problem and defines the retry chain and DLQ
  shape that M8 implements and M17 renders.
- **0007** is about the codebase rather than the runtime; **0008** and **0009** are the two
  decisions the plan explicitly asked to be justified rather than assumed.

## Conventions

**Numbering.** Four digits, monotonically increasing, never reused. `0000` is the template.
Filename: `NNNN-kebab-case-title.md`.

**Status.** One of `Proposed`, `Accepted`, `Superseded by ADR-XXXX`, `Deprecated`. ADRs are
immutable once `Accepted`: to change a decision, write a new ADR and set the old one to
`Superseded by ADR-XXXX`, adding a link in both directions. Typo and link fixes are the only
permitted edits to an accepted ADR.

**Scope.** Write an ADR when a choice is (a) hard to reverse, (b) affects more than one
service, or (c) will be questioned later by someone who was not in the room. Library picks and
naming preferences inside one service do not need one.

**Length.** Under ~80 lines. If it needs more, the decision is probably two decisions.

## Open items to revisit

- Schema Registry compatibility mode (`BACKWARD` is assumed by 0001 and 0002) is confirmed in
  M9 and should get its own ADR if the choice turns out to be non-obvious.
- ~~Kafka security model~~ - recorded as [ADR-0010](0010-kafka-security-model.md).
