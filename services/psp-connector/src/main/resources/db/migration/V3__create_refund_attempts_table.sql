
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

    causation_event_id  UUID          NOT NULL,

    trace_id            VARCHAR(255)  NOT NULL,
    correlation_id      VARCHAR(255)  NOT NULL,
    processed_at        TIMESTAMPTZ   NOT NULL,

    CONSTRAINT uq_refund_attempts_causation_event_id UNIQUE (causation_event_id)
);

CREATE INDEX idx_refund_attempts_refund_id ON refund_attempts (refund_id);
CREATE INDEX idx_refund_attempts_payment_id ON refund_attempts (payment_id);
