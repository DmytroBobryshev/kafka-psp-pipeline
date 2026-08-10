-- V2: transactional outbox table (M6). Owned exclusively by payment-api (ADR-0005), same as
-- `payments` (V1). Written in the SAME Postgres transaction as the payment row it describes
-- (see application.CreatePaymentUseCase) - that atomicity is the entire fix for the dual-write
-- problem documented in services/payment-api/README.md's M3 section: Postgres and Kafka are two
-- separate systems with no shared transaction, so writing "the event that must be published" as
-- a row in THIS database, instead of calling Kafka directly, turns "commit the payment AND
-- publish the event" into a single atomic commit. A separate process (Debezium, via Kafka
-- Connect - infra/compose) tails this table's write-ahead log and republishes each row to Kafka
-- asynchronously, after the fact, with at-least-once delivery.
--
-- Column choices mirror the Debezium outbox-event-router SMT's default field names
-- (io.debezium.transforms.outbox.EventRouter), overridden explicitly in
-- infra/compose/connect/payment-outbox-connector.json to match these exact column names:
--   id             -> transforms.outbox.table.field.event.id        (Kafka Connect header + event id)
--   aggregate_type -> transforms.outbox.route.by.field               (chooses the output topic)
--   aggregate_id   -> transforms.outbox.table.field.event.key        (becomes the Kafka message KEY)
--   payload        -> transforms.outbox.table.field.event.payload    (becomes the Kafka message VALUE)
--   created_at     -> transforms.outbox.table.field.event.timestamp  (becomes the Kafka record timestamp)
--
-- `event_type` is carried for completeness/debugging (visible in AKHQ/psql without joining
-- anything) but is not read by the SMT - routing is driven by `aggregate_type` alone (single
-- event type per aggregate type today; see the connector config for how this generalizes).
--
-- `payload` stores the COMPLETE ADR-0002 envelope + event JSON, byte-identical to what
-- KafkaPaymentEventPublisher (M3, retired from the write path by this migration) used to publish
-- directly - see adapters.out.outbox.OutboxPaymentEventPublisher. The router is configured to
-- expand this JSON and re-emit it unchanged as the record value, so psp-connector's consumer
-- (M4/M5) keeps working with zero changes.
CREATE TABLE outbox_event (
    id             UUID          PRIMARY KEY,
    aggregate_type VARCHAR(255)  NOT NULL,
    aggregate_id   VARCHAR(255)  NOT NULL,
    event_type     VARCHAR(255)  NOT NULL,
    payload        JSONB         NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Debezium reads this table via logical replication (WAL), never via a SELECT, so this index
-- doesn't serve the relay path. It's here for the deferred cleanup job (see README "Known
-- issues"): nothing yet purges old rows, and `created_at` is the natural predicate for that.
CREATE INDEX idx_outbox_event_created_at ON outbox_event (created_at);

-- aggregate_id lookups (e.g. "did this payment's event actually get written to the outbox?"
-- during troubleshooting) are the other natural query shape.
CREATE INDEX idx_outbox_event_aggregate_id ON outbox_event (aggregate_id);
