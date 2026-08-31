-- V9: payment_status_history table (M20). Owned exclusively by payment-api (ADR-0005). One row
-- per payments.payment-status-changed.v1 event this service's own listener has received and
-- applied (adapters.in.kafka.PaymentStatusChangedListener / application.ApplyPaymentOutcomeUseCase)
-- - the full PSP state-machine trail GET /api/payments/{id}/history needs, on top of the payments
-- table's own single "current status" column that same listener has updated since M19.
--
-- event_id is the envelope's own eventId (ADR-0002). The UNIQUE constraint below is the dedup
-- key: a redelivered payments.payment-status-changed.v1 record (at-least-once delivery, a
-- rebalance, an operator replaying the topic) must record ZERO additional rows here, not a
-- duplicate history entry - same "the DB constraint is the authority" idempotent-insert
-- convention psp-connector's payment_attempts.uq_payment_attempts_inbound_event_id (V2) already
-- established for an analogous problem. See
-- adapters.out.persistence.PostgresPaymentStatusHistoryRepository#tryRecord for the
-- try/catch(DataIntegrityViolationException) half of that same convention on this side.
--
-- No CREATED row is ever inserted here: this table only ever hears about a payment AFTER
-- creation - psp-connector is the sole publisher of this topic, and it only starts publishing
-- once a payment already exists. GET /api/payments/{id}/history synthesizes the CREATED entry
-- from the payments row itself instead - see domain.model.PaymentHistoryItem's javadoc and
-- application.PaymentQueryUseCase#history.
--
-- No FK to payments(id) - same convention as refunds.payment_id (V4): the event that drives
-- every insert here can only ever name a payment this service itself created (ADR-0002's
-- aggregateId), so an FK would only catch a bug already impossible by construction.

CREATE TABLE payment_status_history (
    id          UUID        PRIMARY KEY,
    payment_id  UUID        NOT NULL,
    status      VARCHAR(20) NOT NULL,
    event_id    UUID        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT uq_payment_status_history_event_id UNIQUE (event_id)
);

-- GET /api/payments/{id}/history's read path - one lookup per payment, ordered occurredAt asc.
CREATE INDEX idx_payment_status_history_payment_id ON payment_status_history (payment_id);
