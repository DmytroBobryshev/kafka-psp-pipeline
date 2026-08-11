-- V2: the M11 refund saga's local state (ADR-0008 rule 1: each participant persists its own view;
-- there is no shared saga table). Three tables, each with a distinct job - see
-- adapters.out.persistence.RefundWriteTransaction's class javadoc for how they are written
-- together.

-- ---------------------------------------------------------------------------------------------
-- refund_reservations - an immutable audit row, one per SUCCESSFUL reservation. Never updated.
-- ---------------------------------------------------------------------------------------------
-- "A reservation row" per the module brief (docs/PLAN.md M11). Written once, by
-- RefundWriteTransaction#reserveOrFail, only on the branch that actually reserves funds - the
-- insufficient-balance branch never inserts here at all. Whether a reservation is still "active"
-- is derived from refund_saga_state.status = 'RESERVED', not from a column on this table - see
-- domain.model.RefundReservation's javadoc for why a redundant status column here was rejected.
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

-- ---------------------------------------------------------------------------------------------
-- refund_saga_state - the mutable state machine, one row per refund_id. THE row the module brief
-- means by "a refund state row ... with a REST endpoint to read it" (GET /api/refunds/{refundId}).
-- ---------------------------------------------------------------------------------------------
-- Transitioned exclusively via the compare-and-swap UPDATE in RefundSagaStateJpaRepository#
-- transitionIfStatus (UPDATE ... WHERE refund_id = ? AND status = ?) - never a naive
-- load-modify-save, which would race against a concurrent attempt at the same or a conflicting
-- transition (ADR-0008 rule 3: illegal transitions are rejected, not assumed impossible).
--
-- updated_at doubles as "when this row entered its current status" - for a row still RESERVED,
-- that is also "when it was reserved", which is exactly what the TTL sweeper compares against
-- refund.reservation.ttl (idx_refund_saga_state_status_updated_at below).
CREATE TABLE refund_saga_state (
    refund_id     UUID          PRIMARY KEY,
    payment_id    UUID          NOT NULL,
    merchant_id   VARCHAR(255)  NOT NULL,
    amount        NUMERIC(19,4) NOT NULL,
    currency      VARCHAR(3)    NOT NULL,

    -- REQUESTED is never actually written here - this service resolves refunds.refund-requested.v1
    -- to RESERVED or FAILED synchronously, in the same transaction that records the dedup row, so
    -- no external reader could observe an intermediate REQUESTED row even if one were written. See
    -- domain.model.RefundSagaStatus's javadoc.
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

-- The TTL sweeper's exact query shape: WHERE status = 'RESERVED' AND updated_at < :cutoff.
CREATE INDEX idx_refund_saga_state_status_updated_at ON refund_saga_state (status, updated_at);

-- ---------------------------------------------------------------------------------------------
-- refund_processed_events - the idempotency ledger shared by all three refund-saga listeners.
-- ---------------------------------------------------------------------------------------------
-- Generalises ledger_entries.inbound_event_id (M7) across a saga with three distinct consumption
-- points instead of one: refunds.refund-requested.v1, refunds.refund-completed.v1, and
-- refunds.refund-failed.v1 each get their own row here, keyed on THEIR OWN inbound eventId, even
-- though several rows share the same refund_id. inbound_event_id is the primary key - the unique
-- constraint IS the idempotency guarantee, the same role uq_ledger_entries_inbound_event_id plays
-- for M7.
CREATE TABLE refund_processed_events (
    inbound_event_id UUID          PRIMARY KEY,
    refund_id        UUID          NOT NULL,
    event_type       VARCHAR(64)   NOT NULL,
    processed_at     TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_refund_processed_events_refund_id ON refund_processed_events (refund_id);
