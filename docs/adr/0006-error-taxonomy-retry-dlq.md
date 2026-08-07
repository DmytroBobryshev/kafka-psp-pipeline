# ADR-0006: Error taxonomy — retryable vs non-retryable, and the retry/DLQ policy it drives

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** M4, M5, M8 (implements this), M9, M17 (DLQ console)

## Context

A consumer that treats all failures alike does one of two wrong things: retries forever on an
error that can never succeed (the poison pill looping on one offset), or drops a failure a
retry would have fixed. Retry policy is not a config choice — it follows from a classification
of *why* processing failed, and that classification has to exist before M8 can be written. It
must also separate failures from **business outcomes**: a declined card is not an error, it is
the answer.

## Decision

Four categories. Every exception thrown by a listener MUST fall into exactly one.

**A — Retryable (transient infrastructure).** Provider timeout or 5xx, connection reset, DB
deadlock or serialization failure, `SQLTransientException`, Mongo primary step-down, Schema
Registry unreachable, `KafkaException` on produce.
→ Non-blocking retry through the topic chain, then DLQ. Java: extends `RetryableException`.

**B — Business outcome (not an error at all).** Card declined, insufficient funds,
`refund exceeds captured amount`, unknown merchant, validation failure on an event we accepted
at the edge.
→ Emit a domain failure event (`payments.payment-status-changed.v1` with `DECLINED`,
`refunds.refund-failed.v1`), commit the offset, return normally. **Never** retried, **never**
DLQ'd. A DLQ full of declined cards is the classic mistake this category exists to prevent.

**C — Contract violation / poison pill.** Deserialization failure, unknown schema id,
incompatible schema, missing required envelope field, `ClassCastException` on the payload.
→ Straight to DLQ with **zero** retries; the bytes will not improve. Handled by
`ErrorHandlingDeserializer` *before* the listener runs, so it must be configured on every
consumer factory. Java: `NonRetryableException` / `DeserializationException`.

**D — Unknown (bug).** `NullPointerException`, `IllegalStateException`, anything unclassified.
→ Treated as **non-retryable**: one attempt, then DLQ, then alert. Defaulting unknown errors
to "retryable" turns every bug into an infinite loop that stalls a partition.

**Retry chain** (M8, `@RetryableTopic`, non-blocking):

```
<base>  --A--> <base>.retry.5s --A--> <base>.retry.1m --A--> <base>.retry.15m --A--> <base>.dlq
        --B--> commit (no retry)
        --C/D--> <base>.dlq directly
```

Rules:
- **Blocking retries are forbidden.** No `Thread.sleep`, no `RetryTemplate` inside a listener:
  it consumes `max.poll.interval.ms`, triggers a rebalance, and stalls the partition for every
  key, not just the failing one.
- Retry topics are consumed by the **same application** with a delay-until header; delay is
  honoured by pausing the retry-topic consumer, not by sleeping.
- **Every retryable operation MUST be idempotent** (ADR-0003 + M5 dedup), because a retry after
  a partial success is indistinguishable from a first attempt.
- DLQ records MUST carry the original headers plus `x-original-topic/partition/offset`,
  `x-exception-fqcn/message/stacktrace`, `x-attempt-count`, `x-failed-at` (read by the M17
  console).
- **Not every consumer gets a DLQ.** psp-connector, ledger, and webhook-notifier do (their
  work is not reconstructible). `analytics` and `realtime-gateway` are derived, lossy views:
  they log, increment a `records.skipped` counter, and advance the offset. Their state is
  rebuilt by replaying the source topic, not from a DLQ.
- **DLQ is not a queue, it is an inbox.** Replay is manual and explicit (M8 replay endpoint,
  M17 console button), because whatever put a record there usually needs a code change first.
  Replay re-publishes to the original topic with the original key, preserving ADR-0003
  partitioning, and stamps `x-replayed-from` + `x-replay-count`.
- DLQ depth > 0 is an alert. DLQ retention is 30 days — long enough to notice and fix.

## Consequences

**Positive**
- The poison-pill loop (M8's "prove it") becomes a one-line classification question rather
  than a mystery.
- Business failures stay visible in the domain event stream where the UI and analytics can see
  them, instead of hiding in an operational topic.
- Retry latency is bounded and knowable: 5 s + 1 m + 15 m ≈ 16 minutes to DLQ.

**Negative / accepted costs**
- Four extra topics per retry-enabled consumer; the topic count roughly doubles.
- Non-blocking retries **reorder** relative to the base topic: a record retried at 15 m is
  processed long after later records for the same key. Ordering (ADR-0003) therefore holds only
  on the happy path. Consumers whose correctness depends on order (the ledger) must be written
  as idempotent state machines that reject out-of-order transitions rather than assume them.
- Classifying every exception is ongoing work; an unclassified exception silently becomes
  category D and goes to the DLQ. That is the safe default, but it needs the alert.

## Alternatives considered

**Blocking retry with `DefaultErrorHandler` + `BackOff`.** Rejected as the general policy for
the reason above; retained only for sub-second category-A retries, where no rebalance is at
risk.

**Single DLQ for the whole system.** One topic to watch. Rejected: replay needs the original
topic and key, and a shared DLQ makes per-consumer ACLs (M14) impossible.

**Automatic DLQ redrive on a timer.** Convenient, and wrong most of the time — records land in
the DLQ precisely because retrying did not help.

**Scheduled redelivery from a database table instead of retry topics.** Works — webhook-
notifier's Mongo attempt log could drive it. Rejected: it moves the retry mechanism out of
Kafka, which is the thing being learned.
