TRUNCATE TABLE outbox_event;

ALTER TABLE outbox_event
    ALTER COLUMN payload TYPE BYTEA USING NULL::bytea;
