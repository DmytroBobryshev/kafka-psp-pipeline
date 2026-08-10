-- V2: M5 FIX - adds the LEVEL 1 (replay/consumer) idempotency key. V1's
-- uq_payment_attempts_payment_provider_event constraint is LEVEL 2 only: it keys on
-- (payment_id, provider_event_id), and provider_event_id is minted fresh by the provider on
-- every call (see ProviderResult's javadoc) - so it can never catch a straight replay of the
-- SAME inbound payments.payment-requested.v1 record (crash-restart, rebalance, or an operator
-- resetting the consumer group's offsets to earliest and replaying the whole topic). That gap
-- is the proven defect this migration closes - see README.md's M5 section for the measured
-- failure: 50 events replayed once -> 50 processed twice, 0 caught as duplicates, 100
-- payment_attempts rows / 100 distinct provider_event_id / 100 status events for 50 distinct
-- payments.
--
-- inbound_event_id is the inbound EventEnvelope's own eventId (adapters.in.kafka.PaymentRequestedEvent
-- -> application.ProcessPaymentRequestCommand#causationEventId) - stable across replays,
-- rebalances and offset resets, unlike anything the provider mints, which is exactly why it is
-- usable as an idempotency key BEFORE the provider is ever called
-- (application.ProcessPaymentRequestUseCase checks this column first, ahead of
-- PaymentProviderPort#authorize - see that class's javadoc for why the ordering is the whole
-- point of the fix).
--
-- Nullable: V1 predates this column, so any pre-existing rows have no inbound_event_id to
-- backfill (there is no reliable way to reconstruct which inbound eventId originally produced an
-- already-written row). Postgres treats every NULL as distinct for uniqueness purposes, so any
-- number of pre-existing NULL rows can coexist without ever violating the constraint below; every
-- row written from this migration onward always has one - application.ProcessPaymentRequestUseCase
-- never builds a PaymentAttempt without it.

ALTER TABLE payment_attempts
    ADD COLUMN inbound_event_id UUID NULL;

ALTER TABLE payment_attempts
    ADD CONSTRAINT uq_payment_attempts_inbound_event_id UNIQUE (inbound_event_id);
