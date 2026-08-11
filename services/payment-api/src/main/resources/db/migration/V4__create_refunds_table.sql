-- V4: refunds table (M11). Owned exclusively by payment-api (ADR-0005) - payment-api's own local
-- record of what it requested, separate from the ledger's refund_saga_state (ADR-0008 rule 1: each
-- saga participant persists its own view). See domain.model.Refund's javadoc for why this row
-- never advances past "requested".

CREATE TABLE refunds (
    id          UUID          PRIMARY KEY,
    payment_id  UUID          NOT NULL,
    merchant_id VARCHAR(255)  NOT NULL,
    amount      NUMERIC(19,4) NOT NULL,
    currency    VARCHAR(3)    NOT NULL,
    reason      VARCHAR(500),
    created_at  TIMESTAMPTZ   NOT NULL,

    CONSTRAINT ck_refunds_amount_positive CHECK (amount > 0)
);

-- application.RequestRefundUseCase's bounds check sums prior requests for one payment.
CREATE INDEX idx_refunds_payment_id ON refunds (payment_id);
CREATE INDEX idx_refunds_merchant_id ON refunds (merchant_id);
