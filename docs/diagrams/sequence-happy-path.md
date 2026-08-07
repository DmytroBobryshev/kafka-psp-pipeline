# Sequence — happy-path payment, `POST /payments` to SSE update

Topic names and keys are those of [topic-map.md](topic-map.md). The response is `202 Accepted`,
not a result, because of [ADR-0004](../adr/0004-sync-async-boundary.md).

```mermaid
sequenceDiagram
    autonumber
    participant UI as React UI
    participant GW as api-gateway
    participant API as payment-api
    participant PG as Postgres - payment-api
    participant CDC as Debezium / Connect
    participant K as Kafka
    participant PSP as psp-connector
    participant PROV as Acquirer - simulated
    participant LED as ledger
    participant WHK as webhook-notifier
    participant ANA as analytics
    participant RTG as realtime-gateway

    UI->>GW: POST /payments {merchantId, amount, currency}
    Note over GW: injects correlationId + traceparent
    GW->>API: POST /payments
    API->>API: validate, GlobalKTable lookup of merchant config
    rect rgb(240,248,255)
        Note over API,PG: one ACID transaction (M6 outbox)
        API->>PG: INSERT payment (status=REQUESTED)
        API->>PG: INSERT outbox row (PaymentRequested envelope)
    end
    API-->>GW: 202 Accepted {paymentId, status: REQUESTED}
    GW-->>UI: 202 Accepted
    UI->>GW: GET /events/payments/{paymentId} (SSE)
    GW->>RTG: SSE stream open

    CDC->>PG: read WAL, outbox table only
    CDC->>K: produce payments.payment-requested.v1<br/>key=paymentId
    K-->>RTG: payments.payment-requested.v1
    RTG-->>UI: SSE event: REQUESTED
    K-->>ANA: payments.payment-requested.v1

    K-->>PSP: payments.payment-requested.v1
    PSP->>PSP: dedup check (paymentId, providerEventId)
    PSP->>PROV: authorise (100ms-5s)
    PROV-->>PSP: APPROVED + providerRef
    PSP->>PSP: record providerEventId in its own Postgres dedup table
    PSP->>K: produce payments.payment-status-changed.v1<br/>key=merchantId, status=AUTHORISED
    PSP->>PSP: manual ack -> commit offset

    par fan-out to four independent consumer groups
        K-->>RTG: payments.payment-status-changed.v1
        RTG-->>UI: SSE event: AUTHORISED
    and
        K-->>LED: payments.payment-status-changed.v1
        rect rgb(255,248,240)
            Note over LED,K: Kafka transaction (M7)
            LED->>LED: apply to merchant balance (idempotent on eventId)
            LED->>K: produce ledger.ledger-entry-recorded.v1
            LED->>K: sendOffsetsToTransaction + commit
        end
        K-->>RTG: ledger.ledger-entry-recorded.v1
        RTG-->>UI: SSE event: LEDGER_POSTED
    and
        K-->>WHK: payments.payment-status-changed.v1
        WHK->>K: produce webhooks.webhook-delivery-requested.v1
        K-->>WHK: webhooks.webhook-delivery-requested.v1
        WHK->>WHK: POST to merchant endpoint -> 200
        WHK->>WHK: write delivery attempt (Mongo, TTL index)
    and
        K-->>ANA: payments.payment-status-changed.v1
        ANA->>ANA: 1-min tumbling window per merchant, RocksDB + changelog
        ANA->>ANA: upsert projection to Mongo
    end

    UI->>GW: GET /merchants/{id}/metrics
    GW->>ANA: interactive query
    ANA-->>UI: live windowed metrics
```

## What to notice

1. **The 202 arrives before the event is on Kafka.** The commit to Postgres is what makes the
   payment durable; publication is guaranteed afterwards by the outbox, not by the request. Kill
   `payment-api` between the Postgres commit and the CDC read and the event still arrives — that
   is M6's "prove it".
2. **The SSE subscription is opened after the 202**, so the UI can miss the `REQUESTED` event in
   a race. `realtime-gateway` therefore replays the last N events for that `paymentId` on
   subscribe, from its in-memory ring buffer, before streaming live ones.
3. **The key changes when psp-connector produces.** `payment-requested` is keyed by `paymentId` and
   `payment-status-changed` by `merchantId` (ADR-0003). psp-connector is a repartitioning
   producer, and this is exactly why the M13 Streams join needs a repartition topic.
4. **Four consumer groups, one record.** The `par` block is fan-out across groups, not
   parallelism within one. Each group commits independently, so the ledger being slow shows up
   as lag on `ledger.v1` and does not delay the SSE update.
5. **The ledger's Kafka transaction does not cover Postgres.** `sendOffsetsToTransaction` makes
   the produce + offset commit atomic *within Kafka*; the balance update in Postgres is outside
   it and is protected by idempotency on `eventId`. Being able to say this precisely is the M7
   checkpoint.
6. **`webhook-notifier` publishes to itself** — planner produces the delivery command, executor
   consumes it. That hop exists so the retry chain
   attaches to a delivery command rather than to a payment event (ADR-0006).
7. **Each service touches only its own database** (ADR-0005) — psp-connector's dedup table is
   in psp-connector's Postgres, never payment-api's.
8. **Every arrow carries `traceparent`** in Kafka headers, so M15 renders this whole diagram as
   one trace.
