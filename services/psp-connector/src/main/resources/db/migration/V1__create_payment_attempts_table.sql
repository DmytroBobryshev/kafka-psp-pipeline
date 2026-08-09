-- V1: payment_attempts table (M4). Owned exclusively by psp-connector (ADR-0005) - no other
-- service's migrations or connections ever touch this table; the psp_connector Postgres user
-- (infra/compose) cannot even open a connection to another service's database.
--
-- One row per attempt to authorize a payment against the (simulated) provider - including
-- TIMEOUT outcomes, which are recorded but never published as a domain event (ADR-0006). The
-- (payment_id, provider_event_id) unique constraint is the dedup-table SHAPE the module brief
-- asks for; M4 only ever inserts here ("just record attempts"). M5 adds the read-before-write
-- check that turns this into real idempotency - see PaymentAttempt's javadoc.

CREATE TABLE payment_attempts (
    id                  UUID          PRIMARY KEY,
    payment_id          UUID          NOT NULL,
    merchant_id         VARCHAR(255)  NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(3)    NOT NULL,
    provider_event_id   UUID          NOT NULL,
    outcome             VARCHAR(20)   NOT NULL,
    provider_latency_ms BIGINT        NOT NULL,
    causation_event_id  UUID          NOT NULL,
    trace_id            VARCHAR(255)  NOT NULL,
    correlation_id      VARCHAR(255)  NOT NULL,
    processed_at        TIMESTAMPTZ   NOT NULL,

    CONSTRAINT uq_payment_attempts_payment_provider_event UNIQUE (payment_id, provider_event_id)
);

-- The duplicates-vs-loss and end-to-end "prove it" queries both group/count by payment_id.
CREATE INDEX idx_payment_attempts_payment_id ON payment_attempts (payment_id);
