-- Driftline V4 (Shift 5): watermarks, timers, breaks and settlement state, plus the matcher's joins
-- (DESIGN §5.2–§5.6, §6.2, §6.3, §8.1, §12). Text that the application sorts is compared with COLLATE "C", so
-- PostgreSQL orders rows exactly as the in-memory store does, whatever the database's default collation is.

-- The action-owner rule (D11) and the duplicate-capture join (§5.3).
ALTER TABLE payments ADD COLUMN action_owner_break_id text;
CREATE INDEX payments_by_order ON payments (merchant_id, order_id);

-- Stream watermarks per (partition, source) and the settlement watermark per merchant (§5.4). Neither moves back.
CREATE TABLE watermarks (
    partition      int         NOT NULL CHECK (partition >= 0),
    source         text        NOT NULL CHECK (source IN ('GATEWAY', 'MERCHANT')),
    value          timestamptz NOT NULL,
    max_event_time timestamptz,
    last_event_at  timestamptz,
    PRIMARY KEY (partition, source)
);

CREATE TABLE settlement_watermarks (
    merchant_id      text PRIMARY KEY,
    complete_through date NOT NULL
);

CREATE FUNCTION driftline_watermark_forward_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.value < OLD.value
       OR (OLD.max_event_time IS NOT NULL
           AND (NEW.max_event_time IS NULL OR NEW.max_event_time < OLD.max_event_time)) THEN
        RAISE EXCEPTION 'watermarks: p% %: a watermark never moves back', OLD.partition, OLD.source
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER watermarks_forward_only BEFORE UPDATE ON watermarks
    FOR EACH ROW EXECUTE FUNCTION driftline_watermark_forward_only();

CREATE FUNCTION driftline_settlement_watermark_forward_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.complete_through < OLD.complete_through THEN
        RAISE EXCEPTION 'settlement_watermarks: % never moves back', OLD.merchant_id USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER settlement_watermarks_forward_only BEFORE UPDATE ON settlement_watermarks
    FOR EACH ROW EXECUTE FUNCTION driftline_settlement_watermark_forward_only();

-- Timers (§5.5), unique per (merchant, kind, subject); a fired timer is deleted.
CREATE TABLE timers (
    merchant_id text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('ORDER_CHECK', 'DUP_CHECK', 'PENDING_PROBE', 'SETTLEMENT_DUE',
                                                     'BATCH_SOLVE', 'FEED_STALL')),
    subject_key text        NOT NULL,
    partition   int         NOT NULL CHECK (partition >= 0),
    clock       text        NOT NULL CHECK (clock IN ('STREAM_GM', 'STREAM_G', 'SETTLEMENT', 'PROCESSING')),
    fire_at     timestamptz NOT NULL,
    PRIMARY KEY (merchant_id, kind, subject_key)
);
CREATE INDEX timers_due ON timers (partition, clock, fire_at);
CREATE INDEX timers_due_by_merchant ON timers (merchant_id, clock, fire_at);

-- Breaks (§5.6, §8.1): unique per (merchant, type, subject); the id is derived from those three (D4).
CREATE TABLE breaks (
    break_id    text        PRIMARY KEY,
    merchant_id text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('BREAK', 'ANOMALY', 'ALERT')),
    type        text        NOT NULL,
    subject_key text        NOT NULL,
    state       text        NOT NULL CHECK (state IN ('DETECTED', 'AWAITING_EVIDENCE', 'ACTION_PENDING',
                                                      'RESOLVED_AUTO', 'SELF_HEALED', 'ESCALATED',
                                                      'RESOLVED_MANUAL', 'LINKED')),
    detected_at timestamptz NOT NULL,
    evidence    jsonb       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(evidence) = 'object'),
    version     bigint      NOT NULL CHECK (version >= 1),
    UNIQUE (merchant_id, type, subject_key)
);

CREATE TABLE break_transitions (
    break_id   text        NOT NULL REFERENCES breaks (break_id),
    seq        int         NOT NULL CHECK (seq >= 1),
    from_state text,
    to_state   text        NOT NULL,
    at         timestamptz NOT NULL,
    reason     text        NOT NULL,
    PRIMARY KEY (break_id, seq),
    CHECK ((seq = 1) = (from_state IS NULL))
);

-- The settlement worker's receipts (§6.2): one row per distinct upload. At most one ACCEPTED file per merchant and
-- settlement date, and per merchant and file id; a different file for either is stored as REJECTED_CONFLICT.
CREATE TABLE settlement_files (
    sha256          text        PRIMARY KEY CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    merchant_id     text        NOT NULL,
    file_id         text        NOT NULL,
    settlement_date date        NOT NULL,
    line_count      int         NOT NULL CHECK (line_count >= 0),
    total_paise     bigint      NOT NULL CHECK (total_paise >= 0),
    received_at     timestamptz NOT NULL,
    status          text        NOT NULL CHECK (status IN ('ACCEPTED', 'REJECTED_CONFLICT'))
);
CREATE UNIQUE INDEX settlement_files_one_per_date ON settlement_files (merchant_id, settlement_date)
    WHERE status = 'ACCEPTED';
CREATE UNIQUE INDEX settlement_files_one_per_id ON settlement_files (merchant_id, file_id)
    WHERE status = 'ACCEPTED';
CREATE INDEX settlement_files_by_merchant ON settlement_files (merchant_id, file_id);

-- The matcher's view of settlement (§6.2, §6.3), built only from the log.
CREATE TABLE settlement_lines (
    merchant_id     text   NOT NULL,
    file_id         text   NOT NULL,
    line_no         int    NOT NULL CHECK (line_no >= 1),
    settlement_date date   NOT NULL,
    value_date      date   NOT NULL,
    amount_paise    bigint NOT NULL CHECK (amount_paise >= 0),
    fee_paise       bigint CHECK (fee_paise >= 0),
    tax_paise       bigint CHECK (tax_paise >= 0),
    reference       text,
    batch_id        text,
    status          text   NOT NULL CHECK (status IN ('MATCHED', 'SHORT_EXPLAINED', 'SHORT_UNEXPLAINED', 'OVER',
                                                       'CONFLICT', 'UNATTRIBUTED', 'AWAITING_BATCH')),
    PRIMARY KEY (merchant_id, file_id, line_no)
);
CREATE INDEX settlement_lines_by_reference ON settlement_lines (merchant_id, reference) WHERE reference IS NOT NULL;

CREATE TABLE settlement_file_ends (
    merchant_id     text    NOT NULL,
    file_id         text    NOT NULL,
    settlement_date date    NOT NULL,
    line_count      int     NOT NULL CHECK (line_count >= 0),
    total_paise     bigint  NOT NULL CHECK (total_paise >= 0),
    sha256          text    NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    complete        boolean NOT NULL,
    PRIMARY KEY (merchant_id, file_id)
);
CREATE INDEX settlement_file_ends_complete ON settlement_file_ends (merchant_id, settlement_date) WHERE complete;

-- A payment is settled at most once (§6.3).
CREATE TABLE settlement_matches (
    merchant_id text NOT NULL,
    payment_id  text NOT NULL,
    file_id     text NOT NULL,
    line_no     int  NOT NULL CHECK (line_no >= 1),
    how         text NOT NULL CHECK (how IN ('REFERENCED', 'SHORT_EXPLAINED', 'SHORT_PARTIAL', 'OVER_HELD')),
    PRIMARY KEY (merchant_id, payment_id)
);

-- Append-only: break transitions, receipts and matches never change once written.
CREATE TRIGGER break_transitions_append_only BEFORE UPDATE OR DELETE ON break_transitions
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER break_transitions_no_truncate BEFORE TRUNCATE ON break_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER settlement_files_append_only BEFORE UPDATE OR DELETE ON settlement_files
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER settlement_files_no_truncate BEFORE TRUNCATE ON settlement_files
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER settlement_matches_append_only BEFORE UPDATE OR DELETE ON settlement_matches
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER settlement_matches_no_truncate BEFORE TRUNCATE ON settlement_matches
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();

ALTER TABLE watermarks OWNER TO driftline_migrator;
ALTER TABLE settlement_watermarks OWNER TO driftline_migrator;
ALTER TABLE timers OWNER TO driftline_migrator;
ALTER TABLE breaks OWNER TO driftline_migrator;
ALTER TABLE break_transitions OWNER TO driftline_migrator;
ALTER TABLE settlement_files OWNER TO driftline_migrator;
ALTER TABLE settlement_lines OWNER TO driftline_migrator;
ALTER TABLE settlement_file_ends OWNER TO driftline_migrator;
ALTER TABLE settlement_matches OWNER TO driftline_migrator;

REVOKE ALL ON watermarks, settlement_watermarks, timers, breaks, break_transitions, settlement_files, settlement_lines,
    settlement_file_ends, settlement_matches FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON watermarks, settlement_watermarks, breaks, settlement_lines, settlement_file_ends
    TO driftline_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON timers TO driftline_app;
GRANT SELECT, INSERT ON break_transitions, settlement_files, settlement_matches TO driftline_app;
