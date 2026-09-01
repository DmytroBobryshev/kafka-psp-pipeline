
CREATE TABLE refund_status_history (
    id                 UUID        PRIMARY KEY,
    refund_id          UUID        NOT NULL,
    payment_id         UUID        NOT NULL,
    status             VARCHAR(20) NOT NULL,
    provider_reference VARCHAR(64),
    event_id           UUID        NOT NULL,
    occurred_at        TIMESTAMPTZ NOT NULL,
    recorded_at        TIMESTAMPTZ NOT NULL,

    CONSTRAINT uq_refund_status_history_event_id UNIQUE (event_id)
);

CREATE INDEX idx_refund_status_history_refund_id ON refund_status_history (refund_id);
