
CREATE TABLE payment_status_history (
    id          UUID        PRIMARY KEY,
    payment_id  UUID        NOT NULL,
    status      VARCHAR(20) NOT NULL,
    event_id    UUID        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT uq_payment_status_history_event_id UNIQUE (event_id)
);

CREATE INDEX idx_payment_status_history_payment_id ON payment_status_history (payment_id);
