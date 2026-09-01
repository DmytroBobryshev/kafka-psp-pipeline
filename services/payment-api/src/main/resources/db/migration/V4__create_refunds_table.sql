
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

CREATE INDEX idx_refunds_payment_id ON refunds (payment_id);
CREATE INDEX idx_refunds_merchant_id ON refunds (merchant_id);
