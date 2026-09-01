CREATE TABLE outbox_event (
    id             UUID          PRIMARY KEY,
    aggregate_type VARCHAR(255)  NOT NULL,
    aggregate_id   VARCHAR(255)  NOT NULL,
    event_type     VARCHAR(255)  NOT NULL,
    payload        JSONB         NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_event_created_at ON outbox_event (created_at);

CREATE INDEX idx_outbox_event_aggregate_id ON outbox_event (aggregate_id);
