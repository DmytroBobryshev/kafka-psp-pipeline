# ADR-0003: Partition keys and partition counts

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** M3 (the key decision), M7, M10, M11, M13, M18 (KEDA), M19

## Context

Kafka guarantees order **within one partition of one topic** and nowhere else. The key
therefore decides three things at once: which sequences are ordered, how evenly load spreads,
and how many consumer instances can ever run in parallel. It is also effectively immutable —
adding partitions to a live topic re-maps `hash(key) % n` and silently breaks keyed ordering
for keys already in flight (the M19 drill).

The candidates are `paymentId` and `merchantId`. Two facts settle most of it:

1. **A payment belongs to exactly one merchant.** So keying by `merchantId` yields a
   *superset* of `paymentId` ordering: all events of a payment necessarily land in the
   merchant's partition, in order. `merchantId` is the strictly stronger guarantee.
2. **Ordering is vacuous on a topic that carries one event per aggregate.**
   `payments.payment-requested.v1` emits exactly one record per `paymentId`, so no key choice
   can order anything a consumer cares about there.

PSP traffic is Zipfian: a handful of merchants generate most volume, so `merchantId` buys
ordering with hot-partition risk and `paymentId` buys balance with no cross-event ordering.

## Decision

**Default key is `merchantId`.** Exception: topics carrying a single event per aggregate whose
consumer is latency-bound are keyed by `paymentId` / `refundId`.

| Topic | Key | Partitions | What the key buys |
|---|---|---|---|
| `payments.payment-requested.v1` | `paymentId` | 12 | Nothing orderable (one event per payment) — chosen for even spread across psp-connector, whose provider call takes 100 ms–5 s |
| `payments.payment-status-changed.v1` | `merchantId` | 12 | Status transitions ordered per payment **and** per merchant; single-writer per merchant balance in the ledger |
| `refunds.*.v1` | `merchantId` | 6 | Reserve → execute → release ordered per merchant; saga steps hit the same ledger row serially |
| `ledger.ledger-entry-recorded.v1` | `merchantId` | 6 | Entries per merchant in ledger order; analytics aggregates by merchant without a repartition |
| `webhooks.webhook-delivery-requested.v1` (+ retry/DLQ) | `merchantId` | 6 / 6 / 3 | Callbacks to one merchant delivered in event order |
| `merchants.merchant-config-changed.v1` | `merchantId` | 3 | Compaction key = entity id (mandatory for `cleanup.policy=compact`) |
| `psp.provider-status-query/reply.v1` | `paymentId` | 6 | Correlation only; no ordering semantics |
| `*.dlq` | inherited from the original record | 3 | Replay preserves the original key→partition intent |

Global defaults: `replication.factor=3`, `min.insync.replicas=2`, producers `acks=all`,
`enable.idempotence=true`, `max.in.flight.requests.per.connection<=5`.

Partition counts are sized for the **ceiling on consumer parallelism**, not current throughput:
12 on the payment path because psp-connector is the KEDA-scaled service (M18), 6 elsewhere, 3
for low-volume config and DLQ topics. All counts are multiples of 3 so partitions distribute
evenly over 3 brokers.

**Hot-partition escape hatch.** When one merchant saturates a partition, and only then, switch
that topic's key to `<merchantId>#<bucket>` with `bucket = hash(paymentId) % B`, `B = 1` for
every merchant except the whale. Per-merchant ordering survives for everyone else and is
explicitly traded away for the whale, whose ledger writes then rely on M5 idempotency plus row
locking instead of single-writer serialization. Trigger: per-partition lag skew on the M15
dashboard. Do not build it pre-emptively.

## Consequences

**Positive**
- The ledger sees a single in-flight writer per merchant balance, so the EOS work in M7 is
  about Kafka transactions rather than about row contention.
- Analytics aggregates per merchant with no repartition topic on the main path (M10).
- Realtime-gateway can render a payment's full timeline in order from
  `payments.payment-status-changed.v1` alone.

**Negative / accepted costs**
- **Hot partitions are real and expected.** A merchant doing 40 % of volume puts 40 % of
  `payment-status-changed` on one partition; the group's lag is then set by one consumer. This
  is deliberate — the drill in M19 is to observe it.
- **Mixed keys on the payment path.** `payment-requested` (paymentId) joined with
  `payment-status-changed` (merchantId) in M13 is **not co-partitioned**, so Streams must
  `selectKey` + repartition one side. Accepted: it costs one internal topic and is the clearest
  possible demonstration of why co-partitioning exists.
- Ordering never holds **across** topics, so the refund saga must be an idempotent state
  machine keyed on `refundId` rather than an assumption that `funds-reserved` is observed
  before `refund-completed` (ADR-0008).
- Partition counts can only go up, and going up breaks keyed ordering for in-flight keys.

## Alternatives considered

**`paymentId` everywhere.** Near-perfect balance, maximum parallelism, no whale risk. Rejected
because the ledger would then receive concurrent updates to the same merchant balance from N
partitions, turning a single-writer problem into a distributed-locking problem and making the
M7 exactly-once module about Postgres contention instead of Kafka transactions.

**`merchantId` everywhere, including `payment-requested`.** Simpler, one rule. Rejected: it
serializes the slowest consumer in the system (a 5 s provider call) behind one partition per
merchant, capping a busy merchant's authorization throughput at ~0.2–10 payments/s regardless
of how many psp-connector pods KEDA starts. The ordering it would buy there is worth nothing,
because there is one `requested` event per payment.

**No key (round-robin / sticky).** Maximum throughput, zero ordering. Viable only for
`analytics` inputs. Rejected system-wide: the ledger and webhook ordering requirements are
real.

**Custom `Partitioner` doing consistent hashing with weights.** Solves whale skew properly.
Rejected as premature — it hides the skew that M19 is meant to expose, and the composite-key
escape hatch is understandable without a custom partitioner.
