
ALTER TABLE payment_attempts
    ADD COLUMN inbound_event_id UUID NULL;

ALTER TABLE payment_attempts
    ADD CONSTRAINT uq_payment_attempts_inbound_event_id UNIQUE (inbound_event_id);
