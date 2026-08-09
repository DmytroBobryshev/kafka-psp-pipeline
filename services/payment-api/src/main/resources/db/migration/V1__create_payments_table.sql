-- V1: payments table (M3). Owned exclusively by payment-api (ADR-0005) - no other service's
-- migrations or connections ever touch this table; the payment_api Postgres user (infra/compose)
-- cannot even open a connection to another service's database.

CREATE TABLE payments (
    id          UUID PRIMARY KEY,
    merchant_id VARCHAR(255)  NOT NULL,
    amount      NUMERIC(19,4) NOT NULL,
    currency    VARCHAR(3)    NOT NULL,
    status      VARCHAR(20)   NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL
);

-- Not required by M3's single POST/lookup-by-id flow, but merchant-scoped queries are the first
-- thing any later reporting/reconciliation endpoint needs, and an index costs nothing at this
-- table size today.
CREATE INDEX idx_payments_merchant_id ON payments (merchant_id);
