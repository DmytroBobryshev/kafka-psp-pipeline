-- V12: refund_status_history table (M23) - the refund-path mirror of payment_status_history
-- (V9/V10). One row per refunds.refund-status-changed.v1 (PENDING/IPN_RECEIVED/VERIFIED),
-- refunds.refund-completed.v1 (COMPLETED), refunds.refund-failed.v1 (FAILED) or
-- refunds.funds-reserved.v1 (FUNDS_RESERVED) event this service's own listeners have received -
-- history-only, no impact on the refunds table's own REQUESTED-only state (see domain.model.Refund).
--
-- event_id is each event's own envelope eventId; the UNIQUE constraint is the dedup key, same
-- "the DB constraint is the authority" convention as V9's uq_payment_status_history_event_id.
--
-- No REQUESTED row is ever inserted here: GET /api/payments/{paymentId}/refunds/{refundId}/history
-- synthesizes it from the refunds row's own created_at, same as the payment trail synthesizes
-- CREATED from payments.created_at.

CREATE TABLE refund_status_history (
    id                 UUID        PRIMARY KEY,
    refund_id          UUID        NOT NULL,
    payment_id         UUID        NOT NULL,
    status             VARCHAR(20) NOT NULL,
    provider_reference VARCHAR(64),
    event_id           UUID        NOT NULL,
    occurred_at        TIMESTAMPTZ NOT NULL,
    recorded_at        TIMESTAMPTZ NOT NULL,

    CONSTRAINT uq_refund_status_history_event_id UNIQUE (event_id)
);

-- GET /api/payments/{paymentId}/refunds/{refundId}/history's read path.
CREATE INDEX idx_refund_status_history_refund_id ON refund_status_history (refund_id);
