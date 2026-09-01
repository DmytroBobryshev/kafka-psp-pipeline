
CREATE TABLE refund_reservations (
    id            UUID          PRIMARY KEY,
    refund_id     UUID          NOT NULL,
    payment_id    UUID          NOT NULL,
    merchant_id   VARCHAR(255)  NOT NULL,
    amount        NUMERIC(19,4) NOT NULL,
    currency      VARCHAR(3)    NOT NULL,
    reserved_at   TIMESTAMPTZ   NOT NULL,

    CONSTRAINT uq_refund_reservations_refund_id UNIQUE (refund_id),
    CONSTRAINT ck_refund_reservations_amount_positive CHECK (amount > 0)
);

CREATE INDEX idx_refund_reservations_merchant_id ON refund_reservations (merchant_id);

CREATE TABLE refund_saga_state (
    refund_id     UUID          PRIMARY KEY,
    payment_id    UUID          NOT NULL,
    merchant_id   VARCHAR(255)  NOT NULL,
    amount        NUMERIC(19,4) NOT NULL,
    currency      VARCHAR(3)    NOT NULL,

    status        VARCHAR(30)   NOT NULL,
    reason        VARCHAR(255),

    created_at    TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL,

    CONSTRAINT ck_refund_saga_state_amount_positive CHECK (amount > 0),
    CONSTRAINT ck_refund_saga_state_status CHECK (
        status IN ('REQUESTED', 'RESERVED', 'COMPLETED', 'FAILED', 'RELEASED', 'NEEDS_MANUAL_REVIEW')
    )
);

CREATE INDEX idx_refund_saga_state_merchant_id ON refund_saga_state (merchant_id);

CREATE INDEX idx_refund_saga_state_status_updated_at ON refund_saga_state (status, updated_at);

CREATE TABLE refund_processed_events (
    inbound_event_id UUID          PRIMARY KEY,
    refund_id        UUID          NOT NULL,
    event_type       VARCHAR(64)   NOT NULL,
    processed_at     TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_refund_processed_events_refund_id ON refund_processed_events (refund_id);
