-- V3: outbox_event.payload becomes raw bytes (M9 Phase 1). The outbox-serialization decision in
-- services/payment-api/README.md's M9 section is option (a): payment-api now Avro-encodes the
-- payments.payment-requested.v1 event itself (adapters.out.outbox.OutboxPaymentEventPublisher,
-- via KafkaAvroSerializer against Schema Registry) and writes the EXACT Confluent wire-format
-- bytes - 1 magic byte + 4-byte schema id + Avro binary - straight into this column. Debezium's
-- PostgreSQL connector surfaces a BYTEA column as raw Kafka Connect BYTES
-- (binary.handling.mode=bytes, the connector default), and
-- infra/compose/connect/payment-outbox-connector.json now sets
-- value.converter=org.apache.kafka.connect.converters.ByteArrayConverter, which writes a BYTES
-- value to the topic completely unchanged - no JSON, no Connect-side schema inference, no
-- re-encoding anywhere in the relay path.
--
-- Previously (M6) this column was JSONB, populated with Jackson-serialized JSON and expanded by
-- the outbox event router SMT's table.expand.json.payload=true. That flag is now false (see the
-- connector config) since payload is no longer JSON text to parse - it is already the complete,
-- final wire value.
--
-- TRUNCATE first: existing rows (if any) hold JSON text under the old M6 shape, which is not a
-- valid Postgres bytea literal and belongs to a schema/serialization version this outbox no
-- longer produces. This is a throwaway learning-cluster table with no cross-migration replay
-- guarantee (see the M6 README section's "Known issues" - there was never an outbox cleanup job
-- either), so discarding old rows here is the pragmatic choice, not a regression.
TRUNCATE TABLE outbox_event;

ALTER TABLE outbox_event
    ALTER COLUMN payload TYPE BYTEA USING NULL::bytea;
