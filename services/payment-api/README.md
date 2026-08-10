# payment-api (M3 - first producer)

Entry point for the pipeline. Accepts `POST /api/payments` over REST, persists the payment to
PostgreSQL, and publishes a `payments.payment-requested.v1` event to Kafka.

This is the only service that accepts synchronous REST traffic from outside the system
(ADR-0004); everything downstream is event-driven.

## Kafka concepts demonstrated

- `ProducerRecord` key vs value, and why the key determines the partition
- `acks` (0 / 1 / all) and the durability-vs-latency trade-off - measured below
- `enable.idempotence` and the constraints it silently imposes on `acks` and
  `max.in.flight.requests.per.connection`
- Batching: `linger.ms`, `batch.size`, `compression.type`
- Async send with a callback reporting partition and offset
- `min.insync.replicas` as the setting that gives `acks=all` its meaning

## Architecture

```mermaid
flowchart LR
    client([HTTP client]) -->|POST /api/payments| C[PaymentController]
    C -->|MapStruct: DTO to domain| UC["CreatePaymentUseCase<br/>@Transactional (M6)"]
    UC -->|port| R[(PaymentRepository)]
    UC -->|port| P[PaymentEventPublisher]
    R -.implemented by.-> PG[PostgresPaymentRepository<br/>JPA + Flyway]
    P -.implemented by.-> OB["OutboxPaymentEventPublisher (M6)<br/>plain JPA - no Kafka call"]
    PG --> DB[("PostgreSQL<br/>payment_api.payments")]
    OB --> OBT[("PostgreSQL<br/>payment_api.outbox_event")]
    DB -.same DB transaction.- OBT
    OBT -->|logical replication<br/>wal_level=logical| DBZ["Debezium Postgres connector<br/>(Kafka Connect)"]
    DBZ -->|outbox event router SMT| T[["payments.payment-requested.v1<br/>12 partitions, RF=3"]]
```

`KafkaPaymentEventPublisher` (`adapters/out/kafka`) is the M3 adapter that used to sit where
`OutboxPaymentEventPublisher` sits now - it still compiles and is still documented below for
comparison, but it carries no `@Component` annotation any more, so Spring never wires it in. Ask
the same question of any hexagon: "which adapter is Spring actually instantiating for this port?"
- for `PaymentEventPublisher` today, the answer is `OutboxPaymentEventPublisher`, and there is a
single implementation in the application context, not two competing ones.

The `domain/` package depends on nothing but the JDK. `HexagonalArchitectureTest` (ArchUnit)
fails the build if a Spring, Kafka, or JPA import ever appears there.

## Topics

| Topic | Key | Partitions | RF | min.insync.replicas | cleanup.policy | Retention |
|---|---|---|---|---|---|---|
| `payments.payment-requested.v1` | `paymentId` | 12 | 3 | 2 | delete | 7 days |

Keyed by `paymentId`, not `merchantId` - see ADR-0003. One event per aggregate means ordering
within the key is vacuous here, so the key is chosen to spread load evenly and avoid serializing
a large merchant behind a single partition.

## How to run

The compose stack from `infra/compose` must be running first, including `kafka-connect` with the
outbox connector registered (M6) - otherwise payments still get created and their outbox rows
still get written, they just never reach Kafka until the connector is up.

```bash
cd infra/compose && docker compose up -d && ./create-topics.sh && ./register-connector.sh
mvn -pl services/payment-api -am package
java -jar services/payment-api/target/payment-api.jar --spring.profiles.active=docker-compose
```

Listens on **8085**. Ports 8080 (AKHQ) and 8081 (Schema Registry) are taken by the compose stack.

```bash
curl -X POST http://localhost:8085/api/payments \
  -H 'Content-Type: application/json' \
  -d '{"merchantId":"merchant-acme","amount":49.99,"currency":"EUR"}'
```

## Prove it

### 1. End-to-end

`POST` returns `201` and the event lands on the topic keyed by the payment id:

```
HTTP 201  {"id":"1cc464e6-0a88-4365-9312-567ca70e1362","merchantId":"merchant-acme",
           "amount":49.99,"currency":"EUR","status":"CREATED"}

postgres> 1cc464e6-0a88-4365-9312-567ca70e1362|merchant-acme|49.9900|EUR|CREATED

kafka>    1cc464e6-0a88-4365-9312-567ca70e1362 | KEY={"envelope":{...,"eventId":"019fe41d-9de2-7d79-...",
          "eventType":"payments.payment-requested.v1","aggregateId":"1cc464e6-...","source":"payment-api"},
          "paymentId":"1cc464e6-...","merchantId":"merchant-acme","amount":49.99,...}
```

The Kafka message key equals the payment id, as ADR-0003 requires.

### 2. acks throughput comparison

10,000 records at 512 bytes, `linger.ms=5`, `batch.size=16384`, against the 3-broker cluster:

| acks | enable.idempotence | Throughput | Avg latency | p99 | Max |
|---|---|---|---|---|---|
| 0 | false | 58,480 rec/s (28.6 MB/s) | 9.6 ms | 29 ms | 92 ms |
| 1 | false | 61,350 rec/s (30.0 MB/s) | 16.4 ms | 26 ms | 99 ms |
| all | true | 52,632 rec/s (25.7 MB/s) | 26.4 ms | 48 ms | 100 ms |

**Durability costs latency, not throughput.** Average latency nearly triples from `acks=0` to
`acks=all` while throughput moves less than 15% - and `acks=1` even edges out `acks=0` within
noise. The brokers are containers on loopback, so replication is nearly free; on real network
hardware the throughput gap widens considerably. Don't quote these throughput numbers as if
they generalise - quote the latency shape.

`acks=0` and `acks=1` required `enable.idempotence=false`. Modern Kafka defaults idempotence to
true, and idempotence *requires* `acks=all` - the producer refuses to start otherwise. That
constraint is the point: you cannot have idempotent production without full acknowledgement.

### 3. Leader-kill data loss (the important one)

Single-partition RF=3 topic, 60,000 records at 3,000/s, leader stopped ~8s into the run.

**Under `acks=1`, `min.insync.replicas=1`, `retries=0`:**

```
producer reported : 59,984 records sent
actually durable  : 59,358 records
LOST              :    626 records
leader failover   : kafka2 -> kafka3,  Isr: 3,1
```

**626 records the producer was told were written, which no longer exist.** No error, no
exception, no signal. The leader acknowledged them, died before its followers replicated them,
and the new leader never had them. A payment system on `acks=1` loses transactions and reports
success while doing it.

**Under `acks=all`, `min.insync.replicas=2`:**

```
producer reported : 60,000 records sent
actually durable  : 60,000 records
LOST              :      0 records
leader failover   : kafka3 -> kafka1,  Isr: 1,2
latency during failover: max 692 ms, p99 513 ms  (vs max 101 ms, p99 3 ms under acks=1)
```

The 692 ms spike *is* the leader election - producers block until a new leader is elected and
the write can be acknowledged by two replicas. Half a second of stall in exchange for never
silently losing a payment.

### Why `min.insync.replicas=2` is the setting that matters

`acks=all` alone means "all *in-sync* replicas", which is a hollow guarantee: if the ISR has
shrunk to just the leader, `acks=all` silently degrades to `acks=1`. `min.insync.replicas=2`
forces the broker to reject the write instead of accepting it into a single-replica ISR.

In the drill above the ISR shrank from 3 members to 2 and still met the floor, so writes
continued safely. Had a second broker failed, the producer would have received
`NotEnoughReplicasException` - failing loudly rather than losing data quietly.

## M6 - Transactional outbox

### The problem this fixes

Through M3-M5, `CreatePaymentUseCase.execute()` did two things with no shared transaction:
commit the payment row to Postgres, then call Kafka. A crash between those two lines left the
payment durably persisted while `payments.payment-requested.v1` never received the event - no
error, no retry, no trace. That is the **dual-write problem**: whenever a single business
operation must durably affect two different systems (here, a relational database and a Kafka
topic), there is no way to make an ordinary two-step write atomic across both. Either step can
succeed while the other fails, and a network partition or process crash between the two turns
"this payment happened" into a permanent lie the rest of the system never hears about.

### The fix: transactional outbox

The outbox pattern turns the two-system problem into a one-system problem: instead of writing to
Postgres *and* Kafka, `CreatePaymentUseCase.execute()` (now `@Transactional`) writes to Postgres
*twice* - the `payments` row and a new `outbox_event` row - in the **same** database transaction.
Postgres already gives that write atomicity for free (either both rows commit or neither does),
so there is no longer a window where the payment exists without its event. The `outbox_event`
row is not "the event" in the sense of something a consumer reads directly; it is a durable,
transactional *record of intent to publish*, sitting in the same database as the fact it
describes.

Publishing to Kafka happens later, out of process, by tailing that table - see
`adapters/out/outbox/OutboxPaymentEventPublisher` (implements the existing `PaymentEventPublisher`
port; only the adapter changed, not the port) and
`db/migration/V2__create_outbox_event_table.sql`. `KafkaPaymentEventPublisher` (M3) is kept in the
codebase for reference but is no longer a Spring bean - nothing on the POST path talks to Kafka.

### Kafka Connect architecture: worker, connector, task, converter

"Kafka Connect" is a small stack of distinct concepts, all visible in
`infra/compose/docker-compose.yml`'s `kafka-connect` service and
`infra/compose/connect/payment-outbox-connector.json`:

- **Worker** - the JVM process itself (the `kafka-connect` container, `debezium/connect` image).
  A worker can run many connectors; this stack runs one, in *distributed* mode (the production
  mode - a `group.id`-coordinated cluster that happens to have exactly one member here), which is
  why it needs three bookkeeping topics of its own (`connect.configs`, `connect.offsets`,
  `connect.status` - config for compaction-friendly single-partition storage, offsets and status
  for the actual per-connector state). The worker creates these itself via its embedded
  AdminClient on first start, which is why they are deliberately absent from `create-topics.sh`.
- **Connector** - the logical job, registered via the REST API
  (`infra/compose/register-connector.sh` POSTs/PUTs `payment-outbox-connector.json`), specifying
  *what* to capture (`io.debezium.connector.postgresql.PostgresConnector`, pointed at the
  `payment_api` database and `outbox_event` table) and *how* to transform/route it (the SMT
  chain, below). A connector doesn't move data itself - it's a config object plus a factory for
  tasks.
- **Task** - the actual unit of execution the connector's config produces (`tasks.max=1` here,
  since Debezium's Postgres connector is inherently single-task: one logical replication slot,
  one ordered WAL stream, no way to parallelize within one source database). The REST API's
  `/connectors/<name>/status` endpoint reports connector state and per-task state separately -
  a connector can be `RUNNING` while its one task is `FAILED`, which is exactly what happened
  during verification (see "Compromises" below) and is why `register-connector.sh` checks both.
- **Converter** - the (de)serialization layer between Connect's internal `Struct`/schema
  representation and the bytes actually written to Kafka. This connector uses
  `key.converter=StringConverter` (the key is plain-text `paymentId`, matching ADR-0003 and what
  `StringSerializer`/`StringDeserializer` already used) and
  `value.converter=JsonConverter` with `schemas.enable=false` - critical, because `true` wraps
  every value in `{"schema":...,"payload":...}`, which is not what `psp-connector`'s
  `JsonDeserializer` expects.

### What the outbox event router SMT does

An SMT (Single Message Transform) is a small, chainable function Connect applies to every record
between the connector and the converter. `io.debezium.transforms.outbox.EventRouter`
(`transforms.outbox.type` in the connector config) is the one SMT that makes the outbox pattern
work instead of just leaking raw CDC noise onto a topic. Line by line:

| Config key | What it does |
|---|---|
| `table.field.event.id` = `id` | The outbox row's primary key becomes the event's identity (a Kafka header) - and since `OutboxPaymentEventPublisher` sets this column to `envelope.eventId()`, the outbox row's own PK *is* the ADR-0002 idempotency key, not a second independent id. |
| `table.field.event.key` = `aggregate_id` | Sets the **Kafka record key** from this column. `aggregate_id` is the payment id, so the emitted record is keyed by `paymentId` - ADR-0003, unchanged from the retired direct-publish path. |
| `table.field.event.payload` = `payload` | Names the column holding the JSON to republish. |
| `table.expand.json.payload` = `true` | Parses that JSON column (rather than treating it as an opaque string) and promotes its top-level fields (`envelope`, `paymentId`, `merchantId`, `amount`, `currency`, `status`) to be the **entire** outgoing record value - no `outbox_event`-table metadata (id/aggregate_type/created_at) leaks into the payload. Without this flag the router would just forward the raw JSON *string*, which a schemas-disabled `JsonConverter` would then re-encode as a quoted JSON string literal - double-encoded and unreadable by a plain `JsonDeserializer`. |
| `route.by.field` = `aggregate_type` | Chooses which column decides the destination topic. |
| `route.topic.replacement` = `payments.payment-requested.v1` | The literal destination topic - hardcoded rather than templated from `${routedByValue}` because this outbox table currently carries exactly one event type; a second event type sharing this table would need this to become a real per-row mapping. |
| `table.fields.additional.placement` = `event_type:header:eventType` | Copies the `event_type` outbox column onto a Kafka header (`eventType`) without touching the value - a bonus for AKHQ/DLQ triage, mirroring ADR-0002's header-duplication convention (`psp-connector` doesn't read this header; it only reads the value). |

Two settings intentionally do **not** appear, and both were removed for a reason discovered
during verification (see "Compromises"):

- `table.field.event.timestamp` - would set the Kafka record's timestamp metadata from a table
  column, but `EventRouterDelegate` requires that column to be a Connect `INT64` schema, and a
  Postgres `TIMESTAMPTZ` column decodes as a `ZonedTimestamp` string, not an int64. Omitting it
  only affects the record's *transport* timestamp; `envelope.occurredAt` inside the JSON value
  (what everything downstream actually reads) is unaffected.
- `heartbeat.interval.ms` - Debezium's optional periodic "I'm still here" record, written to an
  auto-created `__debezium-heartbeat.<topic.prefix>` topic. With
  `auto.create.topics.enable=false` cluster-wide (deliberate, M2), that topic can never be
  auto-created, so the heartbeat producer failed on every attempt - and that repeated failure
  reliably killed and restarted the task roughly once a minute (visible as a repeating
  `No previous offsets found` in `docker compose logs kafka-connect`). Not needed for a
  single-table, low-volume learning stack; deferred rather than fixed by pre-creating the topic.

### How this changes the delivery guarantee

**Before (M3-M5):** effectively **at-most-once** across the payment-to-event boundary. The
Kafka producer itself is `acks=all`/idempotent once a send is attempted, but the *attempt*
depended on the process surviving between the Postgres commit and the `kafkaTemplate.send()`
call. A crash there was silent, permanent data loss for that one event.

**After (M6):** **at-least-once**, and the loss mode above is closed. The payment row and the
outbox row commit together or not at all - there is no crash window between them any more.
Debezium then guarantees at-least-once delivery from that committed row to Kafka: it reads the
Postgres write-ahead log via a logical replication slot (`pg_replication_slots`), which is itself
durable - the slot retains WAL until Debezium confirms it flushed a position, so a Connect/worker
crash just means it resumes from the last confirmed LSN on restart, replaying (not skipping)
anything unflushed. "At-least-once" is the honest ceiling, not "exactly-once": a crash between
Kafka accepting a record and Debezium/Connect durably committing its own offset can redeliver
that same outbox row once more on restart. `psp-connector`'s M5 idempotency table is what turns
that at-least-once delivery into an exactly-once *effect* - the outbox pattern's contribution is
guaranteeing the event is never silently dropped, not deduplicating it (that was already true
before M6, and stays true after).

### Verified end-to-end

Captured against the real compose stack (`wal_level=logical`, `kafka-connect` up, connector
`payment-outbox-connector` `RUNNING`), not hand-written:

```
$ curl -X POST http://localhost:8085/api/payments -d '{"merchantId":"merchant-m6-final","amount":250.00,"currency":"GBP"}'
HTTP 201 {"id":"8524d9b7-8de0-4100-ad2a-6c7a63dc204c","merchantId":"merchant-m6-final",...}

postgres> SELECT * FROM outbox_event WHERE aggregate_id = '8524d9b7-...';
  aggregate_type=payment  aggregate_id=8524d9b7-8de0-4100-ad2a-6c7a63dc204c
  event_type=payments.payment-requested.v1
  payload={"amount":250.00,"status":"CREATED","currency":"GBP","envelope":{...},"paymentId":"8524d9b7-...","merchantId":"merchant-m6-final"}

kafka-console-consumer --topic payments.payment-requested.v1 --print-key --print-headers:
  HEADERS: id:019fedb7-eae8-7360-89d6-5fb3891b9a53,eventType:payments.payment-requested.v1
  KEY:     8524d9b7-8de0-4100-ad2a-6c7a63dc204c
  VALUE:   {"amount":250.0,"status":"CREATED","currency":"GBP",
            "envelope":{"source":"payment-api","eventId":"019fedb7-eae8-7360-89d6-5fb3891b9a53",
              "traceId":"f60163d6-1aaa-4437-9d05-87c0f2818f6d",
              "eventType":"payments.payment-requested.v1","occurredAt":1.786399681256336E9,
              "aggregateId":"8524d9b7-8de0-4100-ad2a-6c7a63dc204c","eventVersion":1,
              "aggregateType":"payment","correlationId":"f60163d6-1aaa-4437-9d05-87c0f2818f6d"},
            "paymentId":"8524d9b7-8de0-4100-ad2a-6c7a63dc204c","merchantId":"merchant-m6-final"}
```

Key = `paymentId` (ADR-0003, unchanged). Value has the exact `PaymentRequested`/
`PaymentRequestedEvent` shape both `payment-api` and `psp-connector` already agree on (ADR-0002)
- `envelope{eventId,eventType,eventVersion,aggregateId,aggregateType,occurredAt,source,traceId,
correlationId}` plus `paymentId,merchantId,amount,currency,status` at the top level. One
difference from the M3-era message: a **root event's `causationId` is now absent from the JSON
entirely** rather than present as an explicit `"causationId":null` - the outbox router's JSON
schema inference can't assign a type to a field whose only observed value is `null`, so it drops
the field rather than emitting it untyped. This does not break anything downstream: Jackson
treats a missing object-typed record component the same as an explicit `null` unless
`FAIL_ON_MISSING_CREATOR_PROPERTIES` is enabled, which neither service does.

That last claim isn't just asserted - it was verified by running the **real, unmodified**
`psp-connector` (not a hand-rolled test consumer) against this exact topic with a fresh consumer
group reading from `latest`, then posting a payment:

```
psp-connector log:
  Consumed payment-requested paymentId=f08d9d6b-0f7c-44ee-a44a-c965365e34e1 merchantId=merchant-m6-psp-verify
  Provider call paymentId=f08d9d6b-... outcome=TIMEOUT providerEventId=85cf6bd2-...
  Consumed payment-requested paymentId=f08d9d6b-... merchantId=merchant-m6-psp-verify   (redelivery)
  Deduplicated payment attempt reason=REPLAY paymentId=f08d9d6b-... inboundEventId=019fedb9-...
    - already processed, skipping attempt-log write and publish, acknowledging normally
```

`psp-connector` deserialized the envelope, called the simulated provider, and - on the redelivery
its own retry logic produced - correctly deduplicated via its M5 idempotency table. Zero
`DeserializationException`s. The M4/M5 consumer needed no changes.

### Crash proof

Measured on the live stack. Rather than trying to hit a millisecond window between commit and
relay, the window is held open deliberately by pausing the connector - which is the same state
the system is in if the Connect worker is down, restarting, or lagging.

```bash
curl -X PUT localhost:8083/connectors/payment-outbox-connector/pause   # hold the relay
for i in $(seq 1 25); do curl -X POST localhost:8085/api/payments ... ; done
pkill -9 -f payment-api.jar                                            # SIGKILL, no graceful shutdown
curl -X PUT localhost:8083/connectors/payment-outbox-connector/resume
```

| Step | Measure | Result |
|---|---|---|
| connector paused | connector state | `PAUSED` |
| 25 payments POSTed | `outbox_event` rows | **25** |
| 25 payments POSTed | events on `payments.payment-requested.v1` | **0** |
| payment-api SIGKILLed | health endpoint | dead, no graceful shutdown, no flush |
| connector resumed | events published | **25** |
| connector resumed | distinct crash-batch events on topic | **25** |

**Every event survived a process that was killed before it published anything - because it never
had to publish anything.** The `-9` matters: there was no shutdown hook, no producer flush, no
chance to drain a buffer. The event's existence was already guaranteed by the same Postgres
transaction that created the payment, and Debezium relayed it from the write-ahead log afterwards.

Contrast with M3-M5 behaviour: the same kill between `commit()` and `kafkaTemplate.send()` left the
payment durably in the database and the event nowhere at all - silently, with the API having
already returned `201`. That is the dual write, and this is what removes it.

Note the guarantee that is *not* provided: this is still at-least-once. Debezium can re-deliver a
relayed row after a worker restart, so consumers must stay idempotent - which is exactly what M5
built into psp-connector, and why its replay proof matters here too.

## Known issues / deferred

- `occurredAt` serialises as an epoch decimal (`1786238574.050447000`) rather than ISO-8601.
  Round-trips correctly in Java but is poor as a cross-language wire format and reads badly in
  AKHQ. Fix when M9 moves the topics to Avro. (M6 note: the outbox payload preserves this exact
  format - `OutboxPaymentEventPublisher` serializes with the same `JacksonUtils.enhancedObjectMapper()`
  Spring Kafka's `JsonSerializer` used, specifically so the wire shape doesn't change out from
  under `psp-connector`.)
- **Dual write - fixed by this module.** M3-M5 committed the payment to Postgres and *then*
  published to Kafka with no shared transaction; a crash between the two silently lost the event.
  M6 replaces that with the transactional outbox pattern documented above - the payment and its
  outbox row now commit atomically, and Debezium (not this process) relays the outbox row to
  Kafka at-least-once.
- **No outbox cleanup job.** `outbox_event` is insert-only from this service's side; nothing
  purges old rows (`idx_outbox_event_created_at` exists for exactly this future job, unused
  today). Fine at learning-cluster scale, a real problem in production over time.
- **`snapshot.mode=no_data`** means rows already sitting in `outbox_event` at the moment the
  connector is first registered are never relayed - only inserts that happen *after* registration
  are captured. This is the deliberate, textbook-correct choice for an outbox connector (an
  initial-data snapshot would replay old rows as Debezium "read" (`op=r`) operations, which the
  outbox event router's default `table.op.invalid.behavior=warn` silently drops anyway, since the
  router is designed around insert-only CDC semantics) - but it does mean the very first payment
  written before `register-connector.sh` has ever run against a fresh outbox table will not be
  relayed. Verification here always registered the connector once, before posting payments.
- Drill topics `drill.acks1-loss` and `drill.acksall` remain in the cluster holding the
  evidence above; delete them once the results are written up.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Port 8081 was already in use` | Schema Registry owns 8081; this service uses 8085 |
| Producer won't start, complains about `acks` | `enable.idempotence=true` requires `acks=all` |
| Events publish but never appear on the host | Broker `advertised.listeners` - see `infra/compose/README.md` |
| `NotEnoughReplicasException` | Fewer than `min.insync.replicas` brokers in the ISR - a broker is down |
| Payment POSTs succeed, `outbox_event` fills up, but nothing ever appears on `payments.payment-requested.v1` | (M6) Either the connector isn't registered/running (`curl localhost:8083/connectors/payment-outbox-connector/status`) or it was registered against an outbox table that already existed before it started - `snapshot.mode=no_data` only relays rows inserted *after* registration; see "Known issues" |
| Connector `status` shows `connector.state=RUNNING` but `tasks[0].state=FAILED` after fixing the connector config | PUTting a new config does **not** auto-restart an already-FAILED task (a real Kafka Connect gotcha). `register-connector.sh` handles this itself (one automatic restart-and-retry), but a manual `curl -X POST localhost:8083/connectors/payment-outbox-connector/restart?includeTasks=true` is the fix if you're calling the REST API by hand |
| `docker compose logs kafka-connect` full of `Error while fetching metadata ... __debezium-heartbeat.payment-api=UNKNOWN_TOPIC_OR_PARTITION` | Expected if `heartbeat.interval.ms` is ever re-added to the connector config - the heartbeat topic can't auto-create (`auto.create.topics.enable=false` cluster-wide) and the resulting repeated producer failure was observed to restart the task roughly once a minute during verification. Removed from `payment-outbox-connector.json` for exactly this reason |
