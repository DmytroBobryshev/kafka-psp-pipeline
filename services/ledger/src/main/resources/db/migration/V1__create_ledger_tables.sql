-- V1: the ledger's two tables (M7). Owned exclusively by the ledger service (ADR-0005) - no other
-- service's migrations or connections ever touch them; the `ledger` Postgres user (infra/compose)
-- cannot even open a connection to another service's database.
--
-- These two tables are M7's SECOND mechanism, and the more important of the two for balance
-- correctness. The Kafka transaction that surrounds a ledger write covers Kafka only (the produced
-- ledger.ledger-entry-recorded.v1 record and the consumed offsets, both of which live in Kafka).
-- It cannot extend to this database. Everything below exists so that the balance is right anyway.

-- ---------------------------------------------------------------------------------------------
-- ledger_entries - one immutable row per applied inbound event.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    id                 UUID          PRIMARY KEY,

    -- THE idempotency key (ADR-0002: the inbound envelope's own eventId). Stable across replays,
    -- rebalances, aborted Kafka transactions and operator-driven offset resets - which is exactly
    -- the property nothing generated during processing has, and the reason psp-connector's first
    -- M5 attempt (keyed on a provider-minted id) failed to deduplicate anything at all. NOT NULL,
    -- unlike psp-connector's equivalent column: that one had to stay nullable because a prior
    -- migration predated it, whereas this table is born with the constraint and has no legacy rows.
    inbound_event_id   UUID          NOT NULL,

    merchant_id        VARCHAR(255)  NOT NULL,
    payment_id         UUID          NOT NULL,

    -- CREDIT / DEBIT. `amount` is always POSITIVE; direction carries the sign (domain.model.
    -- LedgerEntry enforces this). A signed amount plus a direction would let one movement be
    -- expressed two ways, which is how ledgers acquire silent bugs.
    direction          VARCHAR(10)   NOT NULL,
    amount             NUMERIC(19,4) NOT NULL,
    currency           VARCHAR(3)    NOT NULL,

    trace_id           VARCHAR(255)  NOT NULL,
    correlation_id     VARCHAR(255)  NOT NULL,
    recorded_at        TIMESTAMPTZ   NOT NULL,

    CONSTRAINT ck_ledger_entries_amount_positive CHECK (amount > 0),
    CONSTRAINT ck_ledger_entries_direction CHECK (direction IN ('CREDIT', 'DEBIT')),

    -- The single most important line in this migration. This is what makes replaying
    -- payments.payment-status-changed.v1 a no-op instead of a double-count. Delete the
    -- transactional producer and balances stay correct; delete THIS and no amount of Kafka
    -- exactly-once will save them.
    CONSTRAINT uq_ledger_entries_inbound_event_id UNIQUE (inbound_event_id)
);

-- Audit/verification queries group by merchant ("show me this merchant's entries in order").
CREATE INDEX idx_ledger_entries_merchant_id ON ledger_entries (merchant_id, recorded_at);
-- Joining a ledger entry back to its payment during triage.
CREATE INDEX idx_ledger_entries_payment_id ON ledger_entries (payment_id);

-- ---------------------------------------------------------------------------------------------
-- merchant_balances - the running total, one row per merchant.
-- ---------------------------------------------------------------------------------------------
-- Written ONLY by the atomic upsert in adapters.out.persistence.MerchantBalanceJpaRepository
-- (INSERT ... ON CONFLICT (merchant_id) DO UPDATE SET balance = balance + EXCLUDED.balance), in
-- the SAME Postgres transaction as the ledger_entries insert above. Both or neither: an entry row
-- without its balance delta would be a permanently wrong balance that no replay could repair,
-- because the replay would be deduplicated by that very entry row.
--
-- balance may go negative (M11 refunds), so there is deliberately no CHECK (balance >= 0).
-- entry_count is maintained by the same statement rather than derived, so that a balance which
-- disagrees with COUNT(*) over ledger_entries is a loud signal that something wrote this table
-- outside the intended path.
CREATE TABLE merchant_balances (
    merchant_id  VARCHAR(255)  PRIMARY KEY,
    currency     VARCHAR(3)    NOT NULL,
    balance      NUMERIC(19,4) NOT NULL,
    entry_count  BIGINT        NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL,

    CONSTRAINT ck_merchant_balances_entry_count CHECK (entry_count >= 0)
);
