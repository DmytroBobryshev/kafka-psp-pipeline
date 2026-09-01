-- V10: payment_status_history.provider_reference (M21). Adds stage 3/4 of the panel's trail -
-- IPN_RECEIVED and VERIFIED - as history-only rows: adapters.in.kafka.PaymentStatusChangedMapper
-- now passes every event's own providerReference through (empty string -> NULL), not just the two
-- new statuses - PENDING's is always empty (-> NULL), SUCCEEDED/DECLINED's was always populated on
-- the wire but had nowhere to land here until now.
--
-- Nullable, no backfill: every row inserted before this migration keeps provider_reference = NULL,
-- which is exactly correct - this service never had that value to record for them.

ALTER TABLE payment_status_history ADD COLUMN provider_reference VARCHAR(64);
