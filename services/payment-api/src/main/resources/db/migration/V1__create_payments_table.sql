
CREATE TABLE payments (
    id          UUID PRIMARY KEY,
    merchant_id VARCHAR(255)  NOT NULL,
    amount      NUMERIC(19,4) NOT NULL,
    currency    VARCHAR(3)    NOT NULL,
    status      VARCHAR(20)   NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_payments_merchant_id ON payments (merchant_id);
