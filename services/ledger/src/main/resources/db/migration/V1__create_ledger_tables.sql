
CREATE TABLE ledger_entries (
    id                 UUID          PRIMARY KEY,

    inbound_event_id   UUID          NOT NULL,

    merchant_id        VARCHAR(255)  NOT NULL,
    payment_id         UUID          NOT NULL,

    direction          VARCHAR(10)   NOT NULL,
    amount             NUMERIC(19,4) NOT NULL,
    currency           VARCHAR(3)    NOT NULL,

    trace_id           VARCHAR(255)  NOT NULL,
    correlation_id     VARCHAR(255)  NOT NULL,
    recorded_at        TIMESTAMPTZ   NOT NULL,

    CONSTRAINT ck_ledger_entries_amount_positive CHECK (amount > 0),
    CONSTRAINT ck_ledger_entries_direction CHECK (direction IN ('CREDIT', 'DEBIT')),

    CONSTRAINT uq_ledger_entries_inbound_event_id UNIQUE (inbound_event_id)
);

CREATE INDEX idx_ledger_entries_merchant_id ON ledger_entries (merchant_id, recorded_at);
CREATE INDEX idx_ledger_entries_payment_id ON ledger_entries (payment_id);

CREATE TABLE merchant_balances (
    merchant_id  VARCHAR(255)  PRIMARY KEY,
    currency     VARCHAR(3)    NOT NULL,
    balance      NUMERIC(19,4) NOT NULL,
    entry_count  BIGINT        NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL,

    CONSTRAINT ck_merchant_balances_entry_count CHECK (entry_count >= 0)
);
