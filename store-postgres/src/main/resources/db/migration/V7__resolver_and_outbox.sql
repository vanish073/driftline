-- Shift 8: the resolver's plan on each break, and the transactional outbox (DESIGN §8, §9, §12).

-- ---------------------------------------------------------------------------------------------------------------
-- The resolver's plan: deadline rule, deadline, escalate_by, when to look next, the action (set at most once), and
-- the resolution with its compensation exposure. A break the resolver has not planned yet has none of these.
-- ---------------------------------------------------------------------------------------------------------------

ALTER TABLE breaks
    ADD COLUMN rule_id            text CHECK (rule_id IN ('RBI_UPI_P2M_T5', 'RBI_UPI_P2P_T1', 'INTERNAL_DUP_T1',
                                                          'INTERNAL_ESCALATE_4H')),
    ADD COLUMN deadline_at        timestamptz,
    ADD COLUMN escalate_by        timestamptz,
    ADD COLUMN review_at          timestamptz,
    ADD COLUMN action             text,
    ADD COLUMN resolved_at        timestamptz,
    ADD COLUMN compensation_paise bigint CHECK (compensation_paise >= 0);

ALTER TABLE breaks ADD CONSTRAINT breaks_plan_all_or_none CHECK (
    (rule_id IS NULL AND deadline_at IS NULL AND escalate_by IS NULL AND review_at IS NULL
        AND compensation_paise IS NULL AND action IS NULL AND resolved_at IS NULL)
    OR (rule_id IS NOT NULL AND deadline_at IS NOT NULL AND escalate_by IS NOT NULL AND review_at IS NOT NULL
        AND compensation_paise IS NOT NULL));
ALTER TABLE breaks ADD CONSTRAINT breaks_escalate_by_not_after_deadline CHECK (escalate_by <= deadline_at);

-- The resolver's work queue: open breaks, unplanned first, then by review time.
CREATE INDEX breaks_due_for_review ON breaks (review_at NULLS FIRST, break_id COLLATE "C")
    WHERE state IN ('DETECTED', 'AWAITING_EVIDENCE', 'ACTION_PENDING');

-- breaks.action is set at most once (§8.1).
CREATE FUNCTION driftline_action_set_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.action IS NOT NULL AND NEW.action IS DISTINCT FROM OLD.action THEN
        RAISE EXCEPTION 'breaks.action of % is % and cannot change to %', OLD.break_id, OLD.action, NEW.action
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER breaks_action_set_once BEFORE UPDATE ON breaks
    FOR EACH ROW EXECUTE FUNCTION driftline_action_set_once();

-- ---------------------------------------------------------------------------------------------------------------
-- The outbox (§9.1, §9.2). args and result are flat objects of strings, like break evidence.
-- ---------------------------------------------------------------------------------------------------------------

CREATE TABLE outbox (
    command_id      text        PRIMARY KEY,
    merchant_id     text        NOT NULL,
    break_id        text        REFERENCES breaks (break_id),   -- NULL only for a pending-order probe (§9.2)
    kind            text        NOT NULL CHECK (kind IN ('STATUS_QUERY_PAYMENT', 'STATUS_QUERY_ORDER', 'REFUND',
                                                         'REFUND_LOOKUP', 'MERCHANT_CONFIRM', 'PUBLISH_OBSERVATION',
                                                         'ESCALATION_NOTIFY')),
    args            jsonb       NOT NULL CHECK (jsonb_typeof(args) = 'object'),
    idempotency_key text,
    status          text        NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'DONE')),
    attempt         int         NOT NULL CHECK (attempt >= 0),
    lease_owner     text,
    lease_until     timestamptz,
    next_attempt_at timestamptz NOT NULL,
    first_sent_at   timestamptz,
    result          jsonb       CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK ((status = 'DONE') = (result IS NOT NULL)),
    CHECK (status <> 'IN_FLIGHT' OR lease_owner IS NOT NULL),
    CHECK ((attempt = 0) = (first_sent_at IS NULL))
);
CREATE INDEX outbox_due ON outbox (next_attempt_at, command_id COLLATE "C") WHERE status <> 'DONE';
CREATE INDEX outbox_by_break ON outbox (merchant_id, break_id, command_id COLLATE "C") WHERE break_id IS NOT NULL;

-- A DONE command is a recorded fact: it never changes again, and no command is ever deleted.
CREATE FUNCTION driftline_outbox_done_is_final() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status = 'DONE' THEN
        RAISE EXCEPTION 'outbox command % is DONE and cannot change', OLD.command_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_done_is_final BEFORE UPDATE ON outbox
    FOR EACH ROW EXECUTE FUNCTION driftline_outbox_done_is_final();
CREATE TRIGGER outbox_no_delete BEFORE DELETE ON outbox
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER outbox_no_truncate BEFORE TRUNCATE ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();

REVOKE ALL ON outbox FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON outbox TO driftline_app;

-- The audit chain (§7.6): appends take this transaction-scoped advisory lock, so two appends can never chain to the
-- same previous row. The key is fixed: hashtext('driftline_audit_log').
COMMENT ON TABLE audit_log IS 'append-only, hash-chained; appends lock pg_advisory_xact_lock(hashtext(''driftline_audit_log''))';
