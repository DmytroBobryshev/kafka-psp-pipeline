-- M19 drill 9 follow-up: the outbound status event's identity must survive a crash between
-- recording the attempt and the broker acknowledging the publish, so a redelivery can republish
-- the SAME logical event (same envelope eventId = the downstream idempotency key) instead of
-- silently skipping it (the old behaviour, which lost 3 payments under KEDA scale-in) or minting
-- a fresh id (which the ledger would book twice).
--
-- Nullable on purpose: rows written before this migration have no stored id. A republish of such
-- a legacy row falls back to minting a fresh eventId - the pre-fix behaviour, acceptable for the
-- finite set of old rows, wrong for any new one.

ALTER TABLE payment_attempts ADD COLUMN status_event_id uuid;
ALTER TABLE refund_attempts ADD COLUMN status_event_id uuid;
