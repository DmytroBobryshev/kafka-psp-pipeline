# Sequence — refund saga (choreography), success and compensation

Implements [ADR-0008](../adr/0008-saga-choreography.md). No orchestrator: each service reacts
to an event and publishes its own. All `refunds.*` topics are keyed by `merchantId`
([ADR-0003](../adr/0003-partition-keys-and-counts.md)), so every step for one merchant is
serialized against that merchant's balance.

```mermaid
sequenceDiagram
    autonumber
    participant UI as React UI
    participant GW as api-gateway
    participant API as payment-api
    participant K as Kafka
    participant LED as ledger
    participant PSP as psp-connector
    participant PROV as Acquirer - simulated
    participant WHK as webhook-notifier
    participant ANA as analytics
    participant RTG as realtime-gateway

    UI->>GW: POST /payments/{paymentId}/refunds {amount}
    GW->>API: POST refund
    API->>API: validate against captured amount, write refund + outbox in one tx
    API-->>UI: 202 Accepted {refundId, status: REQUESTED}
    API->>K: refunds.refund-requested.v1 key=merchantId

    K-->>LED: refunds.refund-requested.v1
    LED->>LED: dedup on eventId, state REQUESTED -> RESERVED
    LED->>LED: reserve funds on merchant balance
    LED->>K: refunds.funds-reserved.v1 key=merchantId
    K-->>RTG: refunds.funds-reserved.v1
    RTG-->>UI: SSE: FUNDS_RESERVED

    K-->>PSP: refunds.funds-reserved.v1
    PSP->>PSP: dedup on providerEventId
    PSP->>PROV: execute refund

    alt provider refunds successfully
        PROV-->>PSP: REFUNDED + providerRef
        PSP->>K: refunds.refund-completed.v1 key=merchantId
        K-->>LED: refunds.refund-completed.v1
        LED->>LED: state RESERVED -> SETTLED
        LED->>LED: convert reservation into a debit
        LED->>K: ledger.ledger-entry-recorded.v1
        K-->>WHK: refunds.refund-completed.v1
        WHK->>K: webhooks.webhook-delivery-requested.v1
        K-->>RTG: refunds.refund-completed.v1
        RTG-->>UI: SSE: REFUND_COMPLETED

    else provider declines or fails permanently
        PROV-->>PSP: DECLINED / permanent error
        Note over PSP: ADR-0006 category B - a business outcome,<br/>not a retry and not a DLQ record
        PSP->>K: refunds.refund-failed.v1 key=merchantId, reason
        K-->>LED: refunds.refund-failed.v1
        rect rgb(255,240,240)
            Note over LED,K: compensation
            LED->>LED: state RESERVED -> RELEASED
            LED->>LED: release the reservation, balance restored
            LED->>K: refunds.reservation-released.v1 key=merchantId
        end
        K-->>WHK: refunds.refund-failed.v1
        WHK->>K: webhooks.webhook-delivery-requested.v1
        K-->>RTG: refunds.refund-failed.v1 + reservation-released.v1
        RTG-->>UI: SSE: REFUND_FAILED, RESERVATION_RELEASED

    else provider times out and no outcome ever arrives
        PROV--xPSP: timeout
        Note over PSP: ADR-0006 category A - retried through<br/>the retry chain, then DLQ
        Note over LED: reservation stays RESERVED
        LED->>LED: sweeper finds reservation older than refund.reservation.ttl
        LED->>K: refunds.reservation-released.v1 reason=TIMEOUT
        K-->>RTG: refunds.reservation-released.v1
        RTG-->>UI: SSE: RESERVATION_RELEASED (timeout)
    end

    K-->>ANA: all refunds.* events
    ANA->>ANA: correlate on refundId + causationId -> saga projection
    UI->>GW: GET /refunds/{refundId}/saga
    GW->>ANA: interactive query
    ANA-->>UI: step timeline for the M17 refund tracker
```

## Saga state machine (held in the ledger, per `refundId`)

```mermaid
stateDiagram-v2
    [*] --> REQUESTED
    REQUESTED --> RESERVED: refunds.refund-requested.v1
    RESERVED --> SETTLED: refunds.refund-completed.v1
    RESERVED --> RELEASED: refunds.refund-failed.v1
    RESERVED --> RELEASED: TTL sweeper (timeout)
    SETTLED --> SETTLED: duplicate refund-completed (idempotent no-op)
    RELEASED --> RELEASED: duplicate refund-failed (idempotent no-op)
    SETTLED --> [*]
    RELEASED --> [*]
```

Transitions not on this diagram are **rejected and logged**, not assumed impossible. In
particular `RELEASED --> SETTLED` is illegal: if `refund-completed` arrives after the timeout
sweeper released the reservation, the refund is escalated for manual review rather than
silently applied — the money left the acquirer, but the ledger already un-reserved it.

## What to notice

1. **Nothing here is a rollback.** Once the acquirer has moved money, the compensation writes a
   *new* ledger entry; it never deletes one. Forward recovery only.
2. **Cross-topic order is not guaranteed.** `refunds.refund-completed.v1` and
   `refunds.reservation-released.v1` are different topics; the ledger must handle them in any
   order, which is why every step is an explicit guarded transition.
3. **A declined refund is not an error** — no retry, no DLQ, just
   `refunds.refund-failed.v1` (ADR-0006 category B). A *timeout* is an error and goes through
   the retry chain.
4. **The timeout branch is the one people forget.** Without the TTL sweeper, a lost
   `refund-completed` leaks a reservation forever and the merchant's available balance is
   quietly wrong. The sweeper is a required deliverable of M11, not a nice-to-have.
5. **No single service knows the saga.** The refund tracker in M17 reads a *projection* built by
   analytics from `causationId` chains (ADR-0002). That projection is the price of choreography
   and ships with M11.
6. **`payment-api` produces via the outbox** here too — omitted from the diagram for width; see
   [sequence-happy-path.md](sequence-happy-path.md).
