
ALTER TABLE payment_attempts ADD COLUMN status_event_id uuid;
ALTER TABLE refund_attempts ADD COLUMN status_event_id uuid;
