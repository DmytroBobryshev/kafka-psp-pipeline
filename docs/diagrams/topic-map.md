# Topic map

The authoritative list of topics. Names follow
[ADR-0001](../adr/0001-topic-naming-and-versioning.md), keys and partition counts follow
[ADR-0003](../adr/0003-partition-keys-and-counts.md), retry/DLQ topics follow
[ADR-0006](../adr/0006-error-taxonomy-retry-dlq.md).

**Cluster-wide defaults** (set at broker level, overridden per topic only where the table says
so): `replication.factor=3`, `min.insync.replicas=2`, `cleanup.policy=delete`,
`retention.ms=604800000` (7 d), `compression.type=zstd`, `unclean.leader.election.enable=false`
(flipped deliberately in the M19 drill), producers `acks=all` + `enable.idempotence=true`.

## Business topics

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers |
|---|---|---|---|---|---|---|
| `payments.payment-requested.v1` | `paymentId` | 12 | 7 d | delete | payment-api (via outbox → Debezium) | psp-connector, analytics, realtime-gateway |
| `payments.payment-status-changed.v1` | `merchantId` | 12 | 7 d | delete | psp-connector | ledger, webhook-notifier, analytics, realtime-gateway |
| `refunds.refund-requested.v1` | `merchantId` | 6 | 7 d | delete | payment-api (via outbox → Debezium) | ledger, analytics, realtime-gateway |
| `refunds.funds-reserved.v1` | `merchantId` | 6 | 7 d | delete | ledger | psp-connector, analytics, realtime-gateway |
| `refunds.refund-completed.v1` | `merchantId` | 6 | 7 d | delete | psp-connector | ledger, webhook-notifier, analytics, realtime-gateway |
| `refunds.refund-failed.v1` | `merchantId` | 6 | 7 d | delete | psp-connector | ledger, webhook-notifier, analytics, realtime-gateway |
| `refunds.reservation-released.v1` | `merchantId` | 6 | 7 d | delete | ledger | analytics, realtime-gateway |
| `ledger.ledger-entry-recorded.v1` | `merchantId` | 6 | 30 d | delete | ledger | analytics, realtime-gateway, Connect Mongo sink (audit-trail) |
| `merchants.merchant-config-changed.v1` | `merchantId` | 3 | ∞ (compacted) | compact | payment-api (merchant config API) | psp-connector, webhook-notifier, analytics, ledger — all as `GlobalKTable` |

**Wire format (M9):** `payments.payment-requested.v1` (Phase 1) and, as of Phase 2,
`payments.payment-status-changed.v1` and `ledger.ledger-entry-recorded.v1` are Avro + Schema
Registry, cut in place with no topic-version bump (ADR-0001's breaking-change test does not fire -
field-for-field identical shape, same reasoning phase 1 already established). `refunds.*` and
`merchants.merchant-config-changed.v1` stay JSON until their respective modules are built. See
services/payment-api/README.md (Phase 1), services/psp-connector/README.md and
services/ledger/README.md (Phase 2) for the full decisions, stale-record handling per topic, and
compatibility-mode registration.

## Webhook delivery chain

`webhook-notifier` consumes payment/refund events and republishes one **delivery command** per
merchant endpoint. The retry chain hangs off that command topic, not off the payment topics —
so a merchant with a broken endpoint never causes retry topics on the payment path.

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers |
|---|---|---|---|---|---|---|
| `webhooks.webhook-delivery-requested.v1` | `merchantId` | 6 | 3 d | delete | *retired M9 Phase 2 - see below* | *retired M9 Phase 2* |
| `webhooks.webhook-delivery-requested.v1.retry.5s` | `merchantId` | 6 | 3 d | delete | *retired M9 Phase 2* | *retired M9 Phase 2* |
| `webhooks.webhook-delivery-requested.v1.retry.1m` | `merchantId` | 6 | 3 d | delete | *retired M9 Phase 2* | *retired M9 Phase 2* |
| `webhooks.webhook-delivery-requested.v1.retry.15m` | `merchantId` | 6 | 3 d | delete | *retired M9 Phase 2* | *retired M9 Phase 2* |
| `webhooks.webhook-delivery-requested.v1.dlq` | `merchantId` | 3 | 30 d | delete | *retired M9 Phase 2* | *retired M9 Phase 2 - holds M8's poisoned-record evidence, left frozen and unread by any service* |
| `webhooks.webhook-delivery-requested.v2` | `merchantId` | 6 | 3 d | delete | webhook-notifier (delivery planner) | webhook-notifier (delivery executor) |
| `webhooks.webhook-delivery-requested.v2.retry.5s` | `merchantId` | 6 | 3 d | delete | webhook-notifier | webhook-notifier |
| `webhooks.webhook-delivery-requested.v2.retry.1m` | `merchantId` | 6 | 3 d | delete | webhook-notifier | webhook-notifier |
| `webhooks.webhook-delivery-requested.v2.retry.15m` | `merchantId` | 6 | 3 d | delete | webhook-notifier | webhook-notifier |
| `webhooks.webhook-delivery-requested.v2.dlq` | `merchantId` | 3 | 30 d | delete | webhook-notifier | webhook-notifier replay API (M8) → UI DLQ console (M17) |

**M9 Phase 2:** the delivery chain (base + 3 retry tiers + dlq) is Avro + Schema Registry, on a
NEW `.v2` topic set - the ADR-0001 versioned-topic route, chosen (over cutting `.v1` in place)
because this chain is entirely internal to one service (webhook-notifier produces and consumes
every hop), so a fresh topic set costs zero cross-team dual-write coordination, and it leaves the
`.v1` chain - including `.v1.dlq`, which holds M8's deliberately-poisoned proof records - completely
untouched rather than reused. `.v2.dlq` itself stays JSON (byte-tolerant), unchanged from M8:
`DeadLetterPublishingRecoverer` must be able to republish a genuine poison pill's raw bytes, which
an Avro serializer cannot do. See services/webhook-notifier/README.md's M9 Phase 2 section.
`payments.payment-status-changed.v1` (inbound to the planner) and `ledger.ledger-entry-recorded.v1`
are also Avro as of M9 Phase 2, cut in place (no version bump) - see services/psp-connector/README.md
and services/ledger/README.md's M9 Phase 2 sections for why.

## Dead-letter topics for other consumers

Only consumers whose work is **not reconstructible by replaying the source** get a DLQ
(ADR-0006). `analytics` and `realtime-gateway` deliberately have none: they log, count, and
skip, and are rebuilt by resetting offsets.

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers |
|---|---|---|---|---|---|---|
| `payments.payment-requested.v1.psp-connector.dlq` | original key (`paymentId`) | 3 | 30 d | delete | psp-connector error handler | psp-connector replay API → UI DLQ console |
| `payments.payment-status-changed.v1.ledger.dlq` | original key (`merchantId`) | 3 | 30 d | delete | ledger error handler | ledger replay API → UI DLQ console |

The `<consumer-app>` segment is present here because both base topics have several consumer
groups; it is absent on the webhook DLQ because that topic has exactly one (ADR-0001).

## Request-reply (M12)

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers |
|---|---|---|---|---|---|---|
| `psp.provider-status-query.v1` | `paymentId` | 6 | 1 h | delete | payment-api (`ReplyingKafkaTemplate`) | psp-connector |
| `psp.provider-status-reply.v1` | `paymentId` | 6 | 1 h | delete | psp-connector | payment-api (correlation-id matched, `KafkaHeaders.REPLY_PARTITION`) |

Short retention is intentional: an unconsumed reply is worthless after the caller's timeout.

## Kafka Streams internal topics (analytics)

Created and managed by Kafka Streams from `application.id = analytics-streams.v1`. **Never
create, rename, or hand-edit these** — the names are derived and the application will not find
a renamed store. Listed for lag dashboards, disk sizing, and M18 quota/ACL planning.

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers |
|---|---|---|---|---|---|---|
| `analytics-streams.v1-merchant-metrics-1m-changelog` | `merchantId` (windowed) | 12 | ∞ + `delete.retention.ms` | compact | analytics (Streams) | analytics (state restore) |
| `analytics-streams.v1-merchant-config-store-changelog` | `merchantId` | 3 | ∞ | compact | analytics (Streams) | analytics (state restore) |
| `analytics-streams.v1-saga-state-changelog` | `refundId` | 6 | ∞ | compact | analytics (Streams) | analytics (state restore) |
| `analytics-streams.v1-<node>-repartition` | re-keyed (`paymentId` for the M13 join) | 12 | `retention.ms=-1`, purged via `deleteRecords` | delete | analytics (Streams) | analytics (Streams) |

The repartition topic exists because `payments.payment-requested.v1` is keyed by `paymentId`
while `payments.payment-status-changed.v1` is keyed by `merchantId` (ADR-0003), so the M13
stream-stream join is not co-partitioned and one side must be re-keyed. Its partition count
**must** equal 12 to co-partition with the other side.

Windowed changelogs inherit the window's retention: `windowSize + gracePeriod`, so a 1-minute
tumbling window with a 30 s grace keeps ~90 s of window state plus compaction.

## Kafka Connect internal topics

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers |
|---|---|---|---|---|---|---|
| `connect.configs` | connector name | 1 | ∞ | compact | Connect workers | Connect workers |
| `connect.offsets` | source partition | 25 | ∞ | compact | Connect workers | Connect workers |
| `connect.status` | connector/task id | 5 | ∞ | compact | Connect workers | Connect workers |

`connect.configs` **must** have exactly 1 partition — Connect requires a single-partition,
compacted config topic. Debezium's Postgres connector needs no schema-history topic.

## Consumer groups

| group.id | Application | Subscribes to |
|---|---|---|
| `psp-connector.v1` | psp-connector | `payments.payment-requested.v1`, `refunds.funds-reserved.v1`, `psp.provider-status-query.v1` |
| `ledger.v1` | ledger | `payments.payment-status-changed.v1`, `refunds.refund-requested.v1`, `refunds.refund-completed.v1`, `refunds.refund-failed.v1` |
| `webhook-notifier.planner.v1` | webhook-notifier | `payments.payment-status-changed.v1`, `refunds.refund-completed.v1`, `refunds.refund-failed.v1` |
| `webhook-notifier.executor.v1` | webhook-notifier | `webhooks.webhook-delivery-requested.v2` + the three `.v2` retry topics (M9 Phase 2; was `.v1` through M8) |
| `analytics-streams.v1` | analytics | Streams-managed (`payments.*`, `refunds.*`, `ledger.*`) |
| `payment-api.replies.v1` | payment-api | `psp.provider-status-reply.v1` |
| `realtime-gateway.<instanceId>` | realtime-gateway | `payments.*`, `refunds.*` — **unique per instance**; consumer groups load-split, they do not fan out (M12) |
| `connect-mongo-audit-sink` | Kafka Connect | `ledger.ledger-entry-recorded.v1`, `payments.payment-status-changed.v1` |

Every group sets `enable.auto.commit=false` with manual ack, `auto.offset.reset=earliest`,
`isolation.level=read_committed` (mandatory for consumers of anything the transactional ledger
produces), and `group.instance.id` for static membership on Kubernetes (M19).

## Sizing rationale, in one line each

- **12 partitions** on the payment path: psp-connector is the KEDA-scaled service and its
  provider call is the slowest step, so it needs the highest parallelism ceiling.
- **6** everywhere else: comfortably above expected consumer instance counts, a multiple of 3
  brokers, and cheap to reason about.
- **3** for config and DLQ topics: low volume; a `GlobalKTable` reads every partition anyway,
  so extra partitions buy nothing.
- **Retention 7 d** on the payment path so M5's "reset to earliest and replay" drill works on a
  realistic window; **30 d** for ledger entries (audit) and DLQs (time to notice and fix);
  **3 d** for webhook deliveries (past that, a redelivery is not useful to the merchant);
  **1 h** for replies.

## Change rules

1. Adding a topic means adding a row here **in the same commit**.
2. Partition counts only go up, and going up breaks keyed ordering for in-flight keys — treat
   it as a migration (the M19 drill exists to make this visceral).
3. Adding a consumer group to a topic that has a bare `.dlq` requires renaming that DLQ to
   include the consumer app (ADR-0001).
4. From M18 on, this table is the source for `KafkaTopic` CRs; the CRs are generated from it,
   not maintained separately.
