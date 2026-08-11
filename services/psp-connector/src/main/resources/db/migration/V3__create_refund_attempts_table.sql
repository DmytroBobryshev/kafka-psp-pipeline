-- V3: refund_attempts table (M11). Owned exclusively by psp-connector (ADR-0005) - the refund-path
-- counterpart of payment_attempts (V1/V2). One row per attempt to execute a refund against the
-- (simulated) provider.
--
-- Unlike payment_attempts, this table carries only ONE idempotency constraint - M5 level 1,
-- replay/consumer idempotency, keyed on the inbound refunds.funds-reserved.v1 event's own eventId.
-- See domain.model.RefundAttempt's javadoc for why level 2 (duplicate provider callback, keyed on
-- a provider-minted id) is not replicated here.

CREATE TABLE refund_attempts (
    id                  UUID          PRIMARY KEY,
    refund_id           UUID          NOT NULL,
    payment_id          UUID          NOT NULL,
    merchant_id         VARCHAR(255)  NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(3)    NOT NULL,
    provider_reference  UUID          NOT NULL,
    outcome             VARCHAR(20)   NOT NULL,
    provider_latency_ms BIGINT        NOT NULL,

    -- THE idempotency key (M5 level 1) - the inbound refunds.funds-reserved.v1 envelope's own
    -- eventId. NOT NULL: this table is born with the constraint, no legacy rows to accommodate
    -- (unlike payment_attempts.inbound_event_id, which had to stay nullable - see its V2 migration).
    causation_event_id  UUID          NOT NULL,

    trace_id            VARCHAR(255)  NOT NULL,
    correlation_id      VARCHAR(255)  NOT NULL,
    processed_at        TIMESTAMPTZ   NOT NULL,

    CONSTRAINT uq_refund_attempts_causation_event_id UNIQUE (causation_event_id)
);

CREATE INDEX idx_refund_attempts_refund_id ON refund_attempts (refund_id);
CREATE INDEX idx_refund_attempts_payment_id ON refund_attempts (payment_id);
