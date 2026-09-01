ALTER TABLE outbox_event
    ADD COLUMN trace_parent VARCHAR(64);
