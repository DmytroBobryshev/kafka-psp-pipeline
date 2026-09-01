
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

CREATE INDEX idx_payment_attempts_payment_id ON payment_attempts (payment_id);
