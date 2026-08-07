# ADR-0008: Refund saga uses choreography, not orchestration

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** M11, M17 (refund tracker), and any future multi-service workflow

## Context

A refund spans three services and cannot be one transaction (ADR-0005): the ledger reserves
funds, psp-connector executes at the provider, the ledger then commits or releases the
reservation. An external party moves money in the middle, so there is no rollback — only
compensation.

Two shapes exist. **Orchestration**: one component holds the workflow state machine and issues
commands. **Choreography**: each service reacts to events and publishes its own.

The project's goal is to learn Kafka, and that is not neutral here: an orchestrator
concentrates the interesting logic in one service and reduces Kafka to a transport, while
choreography pushes ordering, idempotency, and partial-failure questions into every
participant.

## Decision

**Choreography.** The refund flow is:

```
payment-api      --refunds.refund-requested.v1-->      ledger
ledger           --refunds.funds-reserved.v1-->        psp-connector
psp-connector    --refunds.refund-completed.v1-->      ledger (commit), webhook-notifier
psp-connector    --refunds.refund-failed.v1-->         ledger (compensate), webhook-notifier
ledger           --refunds.reservation-released.v1-->  analytics, realtime-gateway
```

All refund topics are keyed by `merchantId` (ADR-0003), so every step for a merchant is
serialized against that merchant's balance.

Rules every participant MUST follow:

1. **Saga state is local.** Each service persists its own view of the refund's state
   (`refundId → state`) in its own database. There is no shared saga table.
2. **Every step is idempotent**, keyed on `eventId` (ADR-0002) plus the local dedup table
   (M5). Reserving twice must reserve once; releasing twice must release once.
3. **Every step is an explicit state transition**, and illegal transitions are rejected, not
   assumed impossible. Kafka orders within a partition of **one** topic — `refund-completed`
   and `reservation-released` live on different topics and can be observed in any order.
4. **Compensation, not rollback.** Once the provider has executed a refund, it stays executed.
   A failure after that point moves forward (write a correcting ledger entry), it does not
   undo.
5. **Compensations must not fail permanently.** A failing compensation is category A/D of
   ADR-0006 and ends in the DLQ with an alert; there is no automatic compensation-of-a-
   compensation.
6. **Timeouts are events.** A reservation that is neither committed nor released within
   `refund.reservation.ttl` is swept by a scheduled job in the ledger that publishes
   `refunds.reservation-released.v1` with reason `TIMEOUT`. Without this, a lost
   `refund-completed` leaks the reservation forever.
7. **No cycles.** A service MUST NOT publish to a topic it also consumes in the same saga.

**Observability replaces the missing coordinator.** Because no single service knows the whole
saga, `analytics` builds a saga projection by consuming all `refunds.*` topics and correlating
on `aggregateId` (= `refundId`) and `causationId` (ADR-0002). That projection is what the M17
refund tracker renders. Building this projection is mandatory, not optional — it is the price
of choreography and must be delivered with M11, not after it.

## Consequences

**Positive**
- Every participant stays autonomous: adding a fraud-check step means deploying one new
  consumer, with no change to existing services.
- No coordinator to be a single point of failure or a bottleneck.
- Exercises exactly the Kafka topics this project exists to learn: ordering guarantees,
  idempotent consumers, out-of-order handling, compensating events.

**Negative / accepted costs**
- **The workflow is not written down anywhere in code.** It exists only as the emergent sum of
  five listeners. The sequence diagram in `docs/diagrams/sequence-refund-saga.md` and the
  analytics projection are the only places the whole flow is visible, and both can drift from
  reality.
- **Changing the flow is a multi-service deploy** with an ordering constraint.
- **Cascade risk.** An event storm or accidental cycle is harder to spot than a runaway
  orchestrator; rule 7 plus per-topic produce ACLs (M14) are the guards. Debugging needs trace
  correlation across five services, which makes M15 load-bearing.
- The honest limit: at ~5+ steps, or once the flow needs branching and human approval,
  choreography stops paying. Revisit this ADR rather than stretch it.

## Alternatives considered

**Orchestration with a dedicated saga service** (commands over Kafka, replies over Kafka). The
flow lives in one readable state machine, timeouts and compensation are centralised, and the
refund tracker reads one table instead of a projection. Rejected because it centralises exactly
the logic this project wants distributed, and reduces the other services to RPC handlers with a
Kafka transport — the learning value collapses.

**Temporal / Camunda / Spring Statemachine.** Production-grade, durable timers, visual
workflow. Rejected: a large new runtime whose whole purpose is to hide the distributed-systems
problems being studied.

**Two-phase commit across the ledger and the provider.** Not available — the provider is a
third party with an HTTP API and no prepare phase.

**Optimistic refund, no reservation** (execute at the provider first, adjust the ledger after).
Fewer steps and fewer events. Rejected: it permits refunding funds the merchant does not have,
and removes the compensation path that is the entire point of M11.
