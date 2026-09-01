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
| `refunds.funds-reserved.v1` | `merchantId` | 6 | 7 d | delete | ledger | psp-connector, payment-api, analytics, realtime-gateway |
| `refunds.refund-completed.v1` | `merchantId` | 6 | 7 d | delete | psp-connector | ledger, webhook-notifier, payment-api, analytics, realtime-gateway |
| `refunds.refund-failed.v1` | `merchantId` | 6 | 7 d | delete | psp-connector | ledger, webhook-notifier, payment-api, analytics, realtime-gateway |
| `refunds.refund-status-changed.v1` | `merchantId` | 6 | 7 d | delete | psp-connector | payment-api |
| `refunds.reservation-released.v1` | `merchantId` | 6 | 7 d | delete | ledger | analytics, realtime-gateway |
| `ledger.ledger-entry-recorded.v1` | `merchantId` | 6 | 30 d | delete | ledger | analytics, realtime-gateway, Connect Mongo sink (audit-trail) |
| `merchants.merchant-config-changed.v1` | `merchantId` | 3 | ∞ (compacted) | compact | payment-api (merchant config API, **direct produce — not the outbox**) | analytics (`GlobalKTable`, M10); psp-connector, webhook-notifier, ledger later, same way |
| `disputes.dispute-opened.v1` | `disputeId` | 3 | 30 d | delete | payment-api (dispute API, **direct produce — not the outbox**, same reasoning as merchant config) | analytics (dispute-projection listener, M13) |

**M10 compaction settings** on `merchants.merchant-config-changed.v1`, applied by
`infra/compose/create-topics.sh` via `kafka-configs --alter` (`kafka-topics --create
--if-not-exists` never alters an existing topic's configs) and verified on the cluster:
`min.cleanable.dirty.ratio=0.1` (default 0.5 — at the default, half the log must be obsolete
before the cleaner runs at all, which on a low-volume config topic can be never),
`delete.retention.ms=60000` (default 24 h — how long a *tombstone* survives after the cleaning
pass that could remove it, so consumers that are behind still observe the delete),
`segment.ms=60000` + `segment.bytes=1048576` (defaults 7 d / 1 GiB — the log cleaner never
touches the **active** segment, so with the default a low-volume topic is never compacted at
all), `max.compaction.lag.ms=60000`. The 60 s values make the tombstone drill watchable and are
deliberately wrong for a real cluster. Full explanation:
services/payment-api/README.md's "M10 - Merchant config" section.

**Deletion on this topic is a tombstone** — a record with the merchant's key and a `null` value —
never a `deleted` flag in the value. A flag *is* a value, so compaction would retain it forever
and every downstream `GlobalKTable` would keep a live row for a merchant that no longer exists.

**M13 claim check.** `disputes.dispute-opened.v1` carries a document at or below
`payment-api.disputes.claim-check-threshold-bytes` (default 512 KiB) INLINE in the record's
`InlineDocument` union branch; above the threshold, payment-api uploads the document to MinIO
first (bucket `disputes`, object key = `disputeId`) and the record carries a `ClaimCheckReference`
branch instead - `{bucket, objectKey, sizeBytes, contentType}`, never the bytes. Kept in the
7-partition-and-under, delete-policy tier deliberately: keeping every claim-checked event small
(well under Kafka's default `max.request.size=1 MiB`) is the entire point of the pattern, so this
topic never needs the broker- or consumer-side size knobs (`max.request.size`, `fetch.max.bytes`,
`max.partition.fetch.bytes`) touched for any other topic in the map. See
services/payment-api/README.md's "M13: claim check, measured" section for the real
`RecordTooLargeException` this topic's default limits produce when the claim-check decision is
bypassed, and the measured claim-check success path against a real multi-megabyte document.

**Wire format (M9):** `payments.payment-requested.v1` (Phase 1) and, as of Phase 2,
`payments.payment-status-changed.v1` and `ledger.ledger-entry-recorded.v1` are Avro + Schema
Registry, cut in place with no topic-version bump (ADR-0001's breaking-change test does not fire -
field-for-field identical shape, same reasoning phase 1 already established). `refunds.*` stays
JSON until its module is built. **`merchants.merchant-config-changed.v1` is Avro as of M10** — it
was never migrated, it was *born* Avro, because M10 is the module that first produces to it
(subject `merchants.merchant-config-changed.v1-value`, `BACKWARD`). Its tombstones have no schema
and never touch the registry: a null value never reaches the serializer. See
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

| Name | Key | Partitions | Retention | cleanup.policy | Producers | Consumers | Status |
|---|---|---|---|---|---|---|---|
| `analytics-streams.v1-merchant-metrics-1m-changelog` | `merchantId` (windowed) | 12 | `retention.ms=1200000` (20 min) | **compact,delete** | analytics (Streams) | analytics (state restore) | **exists (M10)** |
| ~~`analytics-streams.v1-merchant-config-store-changelog`~~ | `merchantId` | 3 | ∞ | compact | — | — | **never created — see below (M10)** |
| `analytics-streams.v1-saga-state-changelog` | `refundId` | 6 | ∞ | compact | analytics (Streams) | analytics (state restore) | predicted (M11) |
| `analytics-streams.v1-<node>-repartition` | re-keyed (`paymentId` for the M13 join) | 12 | `retention.ms=-1`, purged via `deleteRecords` | delete | analytics (Streams) | analytics (Streams) | predicted (M13) — **not created by M10** |

**Corrected by M10, measured against the live cluster.** After the M10 topology ran,
`kafka-topics --list | grep analytics` returned exactly **one** topic — the windowed changelog.
Two rows above were predictions this module falsified:

1. **There is no `-merchant-config-store-changelog`.** A `GlobalKTable`'s source topic already
   *is* its changelog: `merchants.merchant-config-changed.v1` is compacted, so it retains the last
   value per key forever, which is precisely a changelog. Kafka Streams therefore marks global
   stores non-logged, and `Materialized.withLoggingEnabled()` on a global table is rejected. This
   is a direct payoff of the compaction decision — the global store costs zero extra topics.
2. **There is no repartition topic in M10.** ADR-0003 keys
   `payments.payment-status-changed.v1` by `merchantId`, which is the aggregation's grouping key,
   and nothing in the topology changes the key (the `GlobalKTable` join preserves it; there is no
   `selectKey`/`map`; the grouping uses `groupByKey()` and not `groupBy((k,v) -> k)`, which would
   set the repartition flag unconditionally). The repartition row stays as an **M13** prediction:
   that module's stream-stream join of `payments.payment-requested.v1` (`paymentId`) against
   `payments.payment-status-changed.v1` (`merchantId`) is genuinely not co-partitioned, so one
   side must be re-keyed, at 12 partitions to match the other side.

The windowed changelog's `cleanup.policy` is `compact,delete` — **both** — not `compact` alone:
compaction keeps the last value per (key, window) while the delete policy ages whole windows out.
Its `retention.ms` is the store's retention (`analytics.windows.store-retention = 15m`) plus
`windowstore.changelog.additional.retention.ms` (`5m`), giving the 20 minutes observed. Kafka's
default for that second term is 24 h; analytics trims it deliberately for the disk budget. A
1-minute tumbling window with a 30 s grace keeps ~90 s of *live* window state (the floor the
store's retention may not go below), and analytics keeps 15 minutes so an interactive query has
some history to show.

Full reasoning, plus the topology as Streams prints it: services/analytics/README.md.

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
| `analytics-streams.v1` | analytics | Streams-managed. **M10 actual:** `payments.payment-status-changed.v1` + `merchants.merchant-config-changed.v1` (the latter via the `GlobalKTable`'s own global thread, which uses no consumer group and commits no offsets — it always reads every partition from the beginning). `refunds.*` / `ledger.*` come with M11/M13. The group id is not configured anywhere: Streams derives it from `application.id`. |
| `payment-api.replies.<instanceId>` | payment-api | `psp.provider-status-reply.v1` — **unique per instance** (M12); a shared group would let a reply land on a partition the SENDING instance's `ReplyingKafkaTemplate` never sees, timing out a request that was actually answered — see services/payment-api/README.md's M12 section |
| `realtime-gateway.<instanceId>` | realtime-gateway | `payments.*`, `refunds.*` — **unique per instance**; consumer groups load-split, they do not fan out (M12) |
| `connect-mongo-audit-sink` | Kafka Connect | `ledger.ledger-entry-recorded.v1`, `payments.payment-status-changed.v1` |
| `payment-api.refund-status-view.v1` / `payment-api.refund-completed-view.v1` / `payment-api.refund-failed-view.v1` / `payment-api.refund-funds-reserved-view.v1` | payment-api | one group each, respectively, on `refunds.refund-status-changed.v1`, `refunds.refund-completed.v1`, `refunds.refund-failed.v1`, `refunds.funds-reserved.v1` — the refund trail's `refund_status_history` projection (M23), same "independent local projection" shape as `payment-api`'s pre-existing (undocumented above) `payment-api.status-view.v1`/`payment-api.merchant-view.v1` groups |
| `analytics.dispute-projection.v1` | analytics | `disputes.dispute-opened.v1` (M13) - a plain single-record `@KafkaListener`, independent of both `analytics-streams.v1` and `analytics.status-audit-batch.v1` above |

Every group sets `enable.auto.commit=false` with manual ack, `auto.offset.reset=earliest`,
`isolation.level=read_committed` (mandatory for consumers of anything the transactional ledger
produces), and `group.instance.id` for static membership on Kubernetes (M19). **Two deliberate
exceptions to `earliest` (M12):** `realtime-gateway.<instanceId>` and `payment-api.replies.<instanceId>`
both use `auto.offset.reset=latest` — both mint a brand-new, never-before-seen `group.id` on every
restart (the point of "unique per instance"), so `earliest` would only ever replay history that
is either useless (no browser was connected to see it) or provably stale (correlated to a request
no live process could have sent) — see both services' READMEs.

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
