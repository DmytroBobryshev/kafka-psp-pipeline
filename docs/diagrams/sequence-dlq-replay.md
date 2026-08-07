# Sequence — retry chain, DLQ, and replay

Implements [ADR-0006](../adr/0006-error-taxonomy-retry-dlq.md). Two paths are shown: a
**retryable** failure walking the retry chain into the DLQ, and a **poison pill** going
straight there. Both end at the same operator loop.

## Path A — retryable failure through the retry chain

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant WHK as webhook-notifier executor
    participant MER as Merchant endpoint
    participant MDB as Mongo - delivery attempts
    participant R5 as ...retry.5s
    participant R1 as ...retry.1m
    participant R15 as ...retry.15m
    participant DLQ as ...dlq
    participant ALERT as Alerting

    K-->>WHK: webhooks.webhook-delivery-requested.v1
    WHK->>MER: POST callback
    MER--xWHK: 503 Service Unavailable
    WHK->>MDB: attempt 1, failed, 503
    Note over WHK: classify -> ADR-0006 category A, retryable
    WHK->>R5: publish with x-attempt-count=1, x-original-offset
    WHK->>K: ack original record, offset advances

    Note over R5,WHK: consumer pauses the partition until<br/>the delay elapses - never Thread.sleep
    R5-->>WHK: after 5s
    WHK->>MER: POST callback (attempt 2)
    MER--xWHK: 503
    WHK->>MDB: attempt 2, failed
    WHK->>R1: publish with x-attempt-count=2

    R1-->>WHK: after 1m
    WHK->>MER: POST callback (attempt 3)
    MER--xWHK: connection timeout
    WHK->>R15: publish with x-attempt-count=3

    R15-->>WHK: after 15m
    WHK->>MER: POST callback (attempt 4)
    MER--xWHK: 503
    WHK->>DLQ: publish + x-original-topic, x-original-partition,<br/>x-original-offset, x-exception-fqcn, x-exception-message,<br/>x-exception-stacktrace, x-attempt-count=4, x-failed-at
    WHK->>MDB: attempt 4, exhausted
    DLQ-->>ALERT: dlq depth > 0
```

## Path B — poison pill, no retries

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka
    participant EHD as ErrorHandlingDeserializer
    participant PSP as psp-connector listener
    participant DLQ as payments.payment-requested.v1.psp-connector.dlq
    participant ALERT as Alerting

    K-->>EHD: record with bytes that do not match any schema id
    EHD->>EHD: deserialization fails, wraps as DeserializationException
    Note over EHD,PSP: the listener is never invoked -<br/>without this deserializer the consumer<br/>would loop forever on one offset
    EHD->>DLQ: publish raw bytes + failure headers, zero retries
    EHD->>K: ack, offset advances
    DLQ-->>ALERT: dlq depth > 0
```

## Path C — operator inspects and replays

```mermaid
sequenceDiagram
    autonumber
    participant OPS as Operator - M17 DLQ console
    participant GW as api-gateway
    participant SVC as Owning service replay API
    participant DLQ as ...dlq
    participant K as Kafka
    participant TGT as Original topic

    OPS->>GW: GET /dlq/topics
    GW->>SVC: list DLQ topics owned by this service
    SVC-->>OPS: topic list + depth per topic

    OPS->>GW: GET /dlq/{topic}/records?limit=50
    GW->>SVC: browse
    SVC->>DLQ: assign partitions, seek to beginning, poll - no group commit
    DLQ-->>SVC: records
    SVC-->>OPS: payload + retry headers + exception + attempt count

    Note over OPS: diagnose. A DLQ record usually needs a<br/>code or config fix first - replay without one<br/>just refills the DLQ.

    OPS->>OPS: fix merchant endpoint config / deploy the fix

    OPS->>GW: POST /dlq/{topic}/replay {offsets or all}
    GW->>SVC: replay request
    loop for each selected record
        SVC->>DLQ: read record at offset
        SVC->>TGT: produce to x-original-topic with the ORIGINAL key<br/>+ x-replayed-from, x-replay-count, new eventId retained
        Note over SVC,TGT: original key preserved so ADR-0003<br/>partitioning and ordering intent survive
    end
    SVC-->>OPS: replayed N records
    TGT-->>K: normal processing resumes
    Note over SVC,DLQ: DLQ offsets are committed only after a<br/>successful produce, so a crash mid-replay<br/>re-replays rather than loses
```

## What to notice

1. **The offset always advances.** At no point does a failing record block its partition. That
   is the whole reason for non-blocking retries — a blocking `Thread.sleep` of 15 minutes would
   blow `max.poll.interval.ms` and trigger the rebalance storm from M4.
2. **Retries reorder.** A record retried at 15 minutes is processed long after later records for
   the same key. Ordering guarantees (ADR-0003) hold on the happy path only, which is why every
   consumer is an idempotent, guarded state machine.
3. **Replay depends on idempotency.** The replayed record carries its **original `eventId`**, so
   the M5 dedup table recognises it if the first attempt partially succeeded. Minting a new
   `eventId` on replay would defeat dedup and double-charge.
4. **Replay is manual by design.** No timer redrives the DLQ; records land there precisely
   because retrying did not help.
5. **Browsing the DLQ does not commit offsets.** The console assigns partitions manually and
   seeks, so inspection never consumes the record.
6. **`analytics` and `realtime-gateway` have no DLQ.** They log, increment
   `records.skipped`, and advance; their state is rebuilt by resetting the group's offsets and
   replaying the source topics.
