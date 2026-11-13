-- Driftline V1: the double-entry ledger, both balance strategies and the audit trail table.
-- DESIGN §7 (ledger), §7.3 layer 3 ("even raw SQL cannot break it"), §7.5 (balances), §7.6 (audit), §12 (data model).

-- Roles. They are cluster-wide and NOLOGIN; deployments grant them to login users.
--   driftline_migrator owns the schema; driftline_app has DML only, with no UPDATE or DELETE on append-only tables.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'driftline_migrator') THEN
        CREATE ROLE driftline_migrator NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'driftline_app') THEN
        CREATE ROLE driftline_app NOLOGIN;
    END IF;
END
$$;

-- ---------------------------------------------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------------------------------------------

CREATE TABLE ledger_accounts (
    account_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id text   NOT NULL,
    code        text   NOT NULL CHECK (code IN ('GATEWAY_RECEIVABLE', 'BANK', 'PAYMENTS_CLEARING', 'SALES',
                                                'GATEWAY_FEES', 'GST_INPUT_CREDIT', 'SUSPENSE',
                                                'SETTLEMENT_DIFFERENCES')),
    UNIQUE (merchant_id, code)
);

CREATE TABLE ledger_transactions (
    txn_id      text        PRIMARY KEY,
    merchant_id text        NOT NULL,
    cause_type  text        NOT NULL CHECK (cause_type IN ('CAPTURE', 'ORDER_MATCH', 'REFUND', 'SETTLEMENT',
                                                           'SETTLEMENT_PARTIAL', 'SUSPENSE_IN', 'SUSPENSE_RELEASE',
                                                           'MANUAL_ADJUSTMENT')),
    cause_id    text        NOT NULL,
    posted_at   timestamptz NOT NULL,
    -- Set by trigger to the creating database transaction; entries may be added only by that transaction.
    created_xid xid8        NOT NULL DEFAULT pg_current_xact_id(),
    UNIQUE (merchant_id, cause_type, cause_id)
);

CREATE TABLE ledger_entries (
    entry_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_id       text   NOT NULL REFERENCES ledger_transactions (txn_id),
    account_id   bigint NOT NULL REFERENCES ledger_accounts (account_id),
    amount_paise bigint NOT NULL CHECK (amount_paise <> 0),
    -- Set by trigger. APPEND_SNAPSHOT folds only entries below the xid horizon (DESIGN §7.5).
    xid          xid8   NOT NULL DEFAULT pg_current_xact_id()
);
CREATE INDEX ledger_entries_by_txn ON ledger_entries (txn_id);
CREATE INDEX ledger_entries_by_account_xid ON ledger_entries (account_id, xid);

-- ROW_LOCK strategy: one balance row per account, updated by every posting (the measured baseline).
CREATE TABLE account_balances (
    account_id    bigint PRIMARY KEY REFERENCES ledger_accounts (account_id),
    balance_paise bigint NOT NULL DEFAULT 0
);

-- APPEND_SNAPSHOT strategy: balance = latest snapshot + entries with xid >= its horizon.
CREATE TABLE account_snapshots (
    account_id    bigint NOT NULL REFERENCES ledger_accounts (account_id),
    horizon_xid   xid8   NOT NULL,
    balance_paise bigint NOT NULL,
    PRIMARY KEY (account_id, horizon_xid)
);

-- The append-only, hash-chained audit trail (DESIGN §7.6). Rows and verify-audit arrive in Shift 9.
CREATE TABLE audit_log (
    seq       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at        timestamptz NOT NULL,
    actor     text        NOT NULL,
    action    text        NOT NULL,
    subject   text        NOT NULL,
    details   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    prev_hash text        NOT NULL,
    hash      text        NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------------------------------------------------
-- Layer 3 of "rejects any unbalanced transaction" (DESIGN §7.3)
-- ---------------------------------------------------------------------------------------------------------------

-- A ledger transaction records the database transaction that created it; callers cannot choose the value.
CREATE FUNCTION ledger_txn_stamp() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.created_xid := pg_current_xact_id();
    RETURN NEW;
END
$$;
CREATE TRIGGER ledger_transactions_stamp BEFORE INSERT ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_txn_stamp();

-- An entry belongs to the database transaction that created its ledger transaction, and to the same merchant.
CREATE FUNCTION ledger_entry_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    txn_xid          xid8;
    txn_merchant     text;
    account_merchant text;
BEGIN
    NEW.xid := pg_current_xact_id();
    SELECT created_xid, merchant_id INTO txn_xid, txn_merchant
      FROM ledger_transactions WHERE txn_id = NEW.txn_id;
    IF NOT FOUND THEN
        RETURN NEW;  -- the foreign key reports the missing transaction
    END IF;
    IF txn_xid <> NEW.xid THEN
        RAISE EXCEPTION 'ledger: entries can be added only by the database transaction that created ledger transaction %',
            NEW.txn_id;
    END IF;
    SELECT merchant_id INTO account_merchant FROM ledger_accounts WHERE account_id = NEW.account_id;
    IF account_merchant IS DISTINCT FROM txn_merchant THEN
        RAISE EXCEPTION 'ledger: transaction % of merchant % cannot post to an account of merchant %',
            NEW.txn_id, txn_merchant, account_merchant;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ledger_entries_guard BEFORE INSERT ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entry_guard();

-- At COMMIT, every ledger transaction touched has at least 2 entries that sum to zero.
CREATE FUNCTION ledger_txn_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    entry_count bigint;
    total       numeric;
BEGIN
    SELECT count(*), coalesce(sum(amount_paise), 0) INTO entry_count, total
      FROM ledger_entries WHERE txn_id = NEW.txn_id;
    IF entry_count < 2 THEN
        RAISE EXCEPTION 'ledger: transaction % has % entries; at least 2 are required', NEW.txn_id, entry_count;
    END IF;
    IF total <> 0 THEN
        RAISE EXCEPTION 'ledger: transaction % does not balance: its entries sum to %', NEW.txn_id, total;
    END IF;
    RETURN NULL;
END
$$;
CREATE CONSTRAINT TRIGGER ledger_transactions_balanced AFTER INSERT ON ledger_transactions
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_txn_balanced();
CREATE CONSTRAINT TRIGGER ledger_entries_balanced AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_txn_balanced();

-- Append-only: no UPDATE, DELETE or TRUNCATE, whoever asks.
CREATE FUNCTION driftline_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'driftline: % is append-only; % is not allowed', TG_TABLE_NAME, TG_OP;
END
$$;
CREATE TRIGGER ledger_accounts_append_only BEFORE UPDATE OR DELETE ON ledger_accounts
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER ledger_accounts_no_truncate BEFORE TRUNCATE ON ledger_accounts
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER ledger_transactions_append_only BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER ledger_transactions_no_truncate BEFORE TRUNCATE ON ledger_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER ledger_entries_append_only BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER ledger_entries_no_truncate BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER account_snapshots_append_only BEFORE UPDATE OR DELETE ON account_snapshots
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER account_snapshots_no_truncate BEFORE TRUNCATE ON account_snapshots
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER audit_log_no_truncate BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();

-- ---------------------------------------------------------------------------------------------------------------
-- Ownership and least privilege
-- ---------------------------------------------------------------------------------------------------------------

ALTER TABLE ledger_accounts OWNER TO driftline_migrator;
ALTER TABLE ledger_transactions OWNER TO driftline_migrator;
ALTER TABLE ledger_entries OWNER TO driftline_migrator;
ALTER TABLE account_balances OWNER TO driftline_migrator;
ALTER TABLE account_snapshots OWNER TO driftline_migrator;
ALTER TABLE audit_log OWNER TO driftline_migrator;
ALTER FUNCTION ledger_txn_stamp() OWNER TO driftline_migrator;
ALTER FUNCTION ledger_entry_guard() OWNER TO driftline_migrator;
ALTER FUNCTION ledger_txn_balanced() OWNER TO driftline_migrator;
ALTER FUNCTION driftline_append_only() OWNER TO driftline_migrator;

REVOKE ALL ON ledger_accounts, ledger_transactions, ledger_entries, account_balances, account_snapshots, audit_log
    FROM PUBLIC;
GRANT SELECT, INSERT ON ledger_accounts, ledger_transactions, ledger_entries, account_snapshots, audit_log
    TO driftline_app;
-- The ROW_LOCK balance rows are a derived cache, updated by every posting under that strategy.
GRANT SELECT, INSERT, UPDATE ON account_balances TO driftline_app;
