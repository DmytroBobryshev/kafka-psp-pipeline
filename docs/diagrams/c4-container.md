# C4 Level 2 — Containers

Rendered as a Mermaid flowchart rather than `C4Container`: at this node count the flowchart
gives usable layout control, and edge labels can carry the exact topic names from
[topic-map.md](topic-map.md).

Read the edges as: **solid arrow = Kafka record**, labelled with the topic; **dotted arrow =
everything else** (HTTP where labelled, datastore access where not). Per
[ADR-0004](../adr/0004-sync-async-boundary.md) no dotted HTTP arrow runs between two services —
the only dotted HTTP arrows are edge-to-service or system-to-outside-world.

## Runtime containers

```mermaid
flowchart LR
    subgraph clients["External"]
        ui["React + TypeScript UI<br/>Vite, TanStack, SSE"]
        merch["Merchant backend"]
        acq["Card acquirer<br/>(simulated)"]
    end

    subgraph edge["Edge"]
        gw["api-gateway<br/>Spring Cloud Gateway<br/>CORS, rate limit, CB, correlation-id"]
        eureka["discovery-server<br/>Eureka — compose profile only<br/>(ADR-0009)"]
    end

    subgraph svcs["Services (Spring Boot, hexagonal)"]
        papi["payment-api<br/>REST in, outbox out"]
        psp["psp-connector<br/>provider calls, dedup"]
        led["ledger<br/>balances, EOS, reservations"]
        whk["webhook-notifier<br/>retry chain + DLQ"]
        ana["analytics<br/>Kafka Streams + projections"]
        rtg["realtime-gateway<br/>SSE fan-out, unique group.id per instance"]
    end

    subgraph kafka["Kafka platform (KRaft, 3 brokers, RF=3, min.insync.replicas=2)"]
        broker[("Kafka cluster")]
        sr["Schema Registry"]
        connect["Kafka Connect<br/>Debezium source + MongoDB sink"]
    end

    subgraph data["Datastores (ADR-0005)"]
        pgP[("Postgres<br/>payment-api + outbox")]
        pgL[("Postgres<br/>ledger")]
        pgS[("Postgres<br/>psp dedup")]
        moW[("Mongo<br/>delivery attempts")]
        moA[("Mongo<br/>projections")]
        moAudit[("Mongo<br/>audit-trail")]
        rocks[("RocksDB<br/>Streams state")]
    end

    ui -.->|"HTTPS / SSE"| gw
    merch -.->|"HTTPS REST"| gw
    gw -.->|"lb:// (compose) or svc DNS (k8s)"| papi
    gw -.->|"SSE proxy"| rtg
    gw -.->|"DLQ console, projections"| whk
    gw -.->|"metrics API"| ana
    gw -.->|"register / resolve"| eureka

    papi -.-> pgP
    connect -.->|"Debezium CDC, outbox table only"| pgP
    connect -->|"payments.payment-requested.v1<br/>refunds.refund-requested.v1<br/>merchants.merchant-config-changed.v1"| broker
    connect -->|"sink: ledger + payments"| moAudit

    broker -->|"payments.payment-requested.v1"| psp
    psp -->|"payments.payment-status-changed.v1"| broker
    psp -.-> pgS
    psp -.->|"HTTPS"| acq

    broker -->|"payments.payment-status-changed.v1<br/>refunds.*"| led
    led -->|"ledger.ledger-entry-recorded.v1<br/>refunds.funds-reserved.v1<br/>refunds.reservation-released.v1"| broker
    led -.-> pgL

    broker -->|"payments.payment-status-changed.v1<br/>refunds.refund-completed/failed.v1"| whk
    whk -->|"webhooks.webhook-delivery-requested.v1<br/>+ retry.5s / 1m / 15m / dlq"| broker
    whk -.->|"HTTPS callback"| merch
    whk -.-> moW

    broker -->|"payments.*, refunds.*, ledger.*"| ana
    ana -.-> moA
    ana -.-> rocks

    broker -->|"payments.*, refunds.*"| rtg
    rtg -.->|"SSE"| gw

    broker -->|"merchants.merchant-config-changed.v1<br/>(compacted, GlobalKTable)"| psp
    broker -->|"merchants.merchant-config-changed.v1"| whk
    papi -->|"psp.provider-status-query.v1"| broker
    broker -->|"psp.provider-status-reply.v1"| papi

    psp -.-> sr
    papi -.-> sr
    led -.-> sr
    ana -.-> sr
```

## Container responsibilities

| Container | Tech | Owns | Kafka role |
|---|---|---|---|
| `api-gateway` | Spring Cloud Gateway | Routing, CORS, Redis rate limiting, Resilience4j, correlation-id injection | none — never touches Kafka |
| `discovery-server` | Eureka | Registry, **compose profile only** ([ADR-0009](../adr/0009-service-discovery-per-profile.md)) | none |
| `payment-api` | Spring Boot + Postgres | Payment and refund aggregates, merchant config API, outbox table | producer (via Debezium); request side of M12 request-reply |
| `psp-connector` | Spring Boot + Postgres | Provider integration, dedup table `(paymentId, providerEventId)` | consumer + producer; KEDA-scaled on lag (M18) |
| `ledger` | Spring Boot + Postgres | Merchant balances, refund reservations | transactional consumer/producer, `read_committed` (M7) |
| `webhook-notifier` | Spring Boot + Mongo | Merchant callback delivery, attempt log with TTL index | consumer + producer of its own retry chain (M8) |
| `analytics` | Kafka Streams + Mongo + RocksDB | Windowed metrics, saga projection, interactive queries | Streams application `analytics-streams.v1` |
| `realtime-gateway` | Spring Boot, stateless | SSE fan-out to browsers | **unique `group.id` per instance** — broadcast, not load-split (M12) |
| Kafka Connect | Debezium + Mongo sink | Outbox → Kafka; Kafka → audit-trail | source + sink |
| Schema Registry | Confluent | Avro subjects, `BACKWARD` compatibility | — |

## Things this diagram is asserting

1. **`payment-api` never produces directly to Kafka.** It writes to `outbox` in the same
   transaction as the aggregate; Debezium publishes (M6). This is why the arrow goes through
   Connect.
2. **`realtime-gateway` is the one consumer that is not a consumer group in the usual sense.**
   Every instance needs every record, so each takes a unique `group.id`.
3. **`merchants.merchant-config-changed.v1` fans out to several services as a `GlobalKTable`**,
   which is what replaces a "merchant service" REST call.
4. **`api-gateway` has no Kafka client.** The synchronous request-reply of M12 originates in
   `payment-api`, not at the edge.
5. **No HTTP arrow crosses from one service directly to another.** If one ever appears,
   ADR-0004 has been violated.
6. **The only synchronous cross-service call is a Kafka round trip** —
   `psp.provider-status-query.v1` out, `psp.provider-status-reply.v1` back (M12).
