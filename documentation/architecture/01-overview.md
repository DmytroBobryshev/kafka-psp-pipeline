# System overview

## What this is

A miniature **payment service provider (PSP)** built as an event-driven system on Apache Kafka.
Merchants take payments and refunds; the platform authorizes them against a (simulated) payment
provider, keeps double-entry style balances, notifies merchants over webhooks, and shows
everything live in a web UI. Every piece exists to demonstrate a specific Kafka concept on a
realistic domain — see [05-kafka-concepts-map.md](05-kafka-concepts-map.md).

## Services

| Service | Role | Storage |
|---|---|---|
| **payment-api** | REST entry point: payments, refunds, merchant config, history trails. Publishes `payment-requested` via a transactional **outbox** (Debezium). Sweeps payment/refund **expiration**. | PostgreSQL |
| **psp-connector** | Talks to the (simulated) provider. Consumes requests, publishes the full status lifecycle (`PENDING → IPN_RECEIVED → VERIFIED → terminal`). Idempotent via a dedup attempt log. Scales on lag via **KEDA**. | PostgreSQL |
| **ledger** | Money truth. Consumes terminal payment statuses inside a **Kafka transaction** (exactly-once), runs the refund saga's funds reservation, TTL-sweeps stuck reservations. | PostgreSQL |
| **webhook-notifier** | Plans and delivers merchant webhooks with non-blocking **retry topics + DLQ**. Resolves each merchant's configured URL at delivery time. | MongoDB |
| **analytics** | **Kafka Streams**: 1-minute tumbling windows per merchant (decline rate, latency), a `GlobalKTable` join with merchant config, Mongo projection of closed windows. | RocksDB + MongoDB |
| **realtime-gateway** | Fans events out to the browser over SSE (the "broadcast problem"), plus request-reply provider-status queries and cluster-ops endpoints. | — |
| **api-gateway / discovery-server** | Spring Cloud Gateway + Eureka: one REST door into the system. | — |
| **ui** | React + TypeScript: transactions panel, dashboard, simulator, merchant CRUD, DLQ console, cluster view. | — |
| **audit-trail** | A Kafka Connect **sink** into Mongo — an audit log with zero application code. | MongoDB |

## Topics (business)

```
payments.payment-requested.v1        payment-api ──▶ psp-connector        (outbox/Debezium)
payments.payment-status-changed.v1   psp-connector + payment-api(sweep) ──▶ everyone
merchants.merchant-config-changed.v1 payment-api ──▶ projections          (COMPACTED)
refunds.refund-requested.v1          payment-api ──▶ ledger
refunds.funds-reserved.v1            ledger ──▶ psp-connector
refunds.refund-status-changed.v1     psp-connector + payment-api(sweep) ──▶ history/webhooks
refunds.refund-completed.v1 / refund-failed.v1   psp-connector ──▶ ledger, notifier, payment-api
refunds.reservation-released.v1      ledger (TTL sweep / failure compensation)
ledger.entry-recorded.v1             ledger ──▶ audit sink
providerstatus.query.v1 / reply.v1   request-reply (M12)
disputes.dispute-opened.v1           claim-check payloads via MinIO (M13)
+ retry/DLQ topics for webhook delivery
```

## Design principles

1. **Events between services, commands only at the edge.** The only synchronous entry is REST
   through the gateway; everything inter-service is a Kafka event (ADR-0004).
2. **Database per service.** No shared schemas; every read model is a projection built from
   topics (ADR-0005).
3. **The topic is the contract.** Avro + Schema Registry, envelope with eventId / traceId /
   correlationId / causationId (ADR-0002). Adding a field means a default; consumers with old
   schemas keep working.
4. **History is event-sourced, state is a projection.** `payments.status` is a small state
   machine; the full trail (`payment_status_history`, `refund_status_history`) records every
   lifecycle event including ones that never touch state.
5. **Deny-by-default security.** SASL/SCRAM-SHA-512 per service, ACLs granted per topic per
   operation, mirrored between the compose and k8s stacks (M14).
6. **Everything is measured.** Failure drills, quotas, lag autoscaling and loss regressions are
   verified against the live cluster and written down with real outputs (`docs/M19-*`, `M20-*`).
