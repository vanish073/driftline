-- Driftline V5: what the ingest API, the Kafka partition driver and the settlement worker need (Shift 6).
-- DESIGN §10 (ingest), §12 (data model), §14 (token scopes), §5.7 (quarantine of undecodable records), D47, D48.

-- ---------------------------------------------------------------------------------------------------------------
-- Merchants (§12, D5): the partition is fixed at onboarding; the terms are what the matcher's MerchantDirectory holds.
-- §12 also names fixed_fee_paise and cutoff_local_time; the v1 fee model has neither (FeeSchedule is bps plus GST), so
-- they are not stored until a model uses them (D48).
-- ---------------------------------------------------------------------------------------------------------------
CREATE TABLE merchants (
    merchant_id           text        PRIMARY KEY CHECK (merchant_id ~ '^[A-Za-z0-9._-]{1,64}$'),
    partition             int         NOT NULL CHECK (partition >= 0),
    fee_bps               bigint      NOT NULL CHECK (fee_bps >= 0 AND fee_bps <= 10000),
    gst_bps               bigint      NOT NULL CHECK (gst_bps >= 0 AND gst_bps <= 10000),
    cycle_days            int         NOT NULL CHECK (cycle_days >= 0 AND cycle_days <= 30),
    calendar_id           text        NOT NULL CHECK (calendar_id IN ('DAILY')),
    first_settlement_date date        NOT NULL,
    onboarded_at          timestamptz NOT NULL
);

-- Webhook secrets (§10.2): at most two active per merchant, so a secret can be rotated. secret_enc is AES-256-GCM
-- ciphertext (key id, nonce, ciphertext and tag); the key lives outside the database (D48).
CREATE TABLE merchant_webhook_secrets (
    merchant_id text        NOT NULL REFERENCES merchants (merchant_id),
    kid         text        NOT NULL CHECK (kid ~ '^[A-Za-z0-9_.-]{1,64}$'),
    secret_enc  bytea       NOT NULL CHECK (octet_length(secret_enc) BETWEEN 60 AND 1024),
    active_from timestamptz NOT NULL,
    active_to   timestamptz,
    PRIMARY KEY (merchant_id, kid),
    CHECK (active_to IS NULL OR active_to > active_from)
);

-- API tokens (§14): opaque bearer tokens, stored only as SHA-256. merchant_id NULL is an ops token.
CREATE TABLE api_tokens (
    token_sha256 text        PRIMARY KEY CHECK (token_sha256 ~ '^[0-9a-f]{64}$'),
    merchant_id  text        REFERENCES merchants (merchant_id),
    scopes       text[]      NOT NULL CHECK (cardinality(scopes) >= 1 AND scopes <@ ARRAY['events:write',
                                 'settlement:write', 'observations:write', 'breaks:read', 'breaks:resolve']::text[]),
    created_at   timestamptz NOT NULL,
    revoked_at   timestamptz,
    CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

-- ---------------------------------------------------------------------------------------------------------------
-- The settlement upload inbox (§6.2 step RECEIVED, D38, D48). The upload endpoint stores the raw bytes and answers;
-- the separate settlement worker parses them, records the receipt (settlement_files) and emits to Kafka. One row per
-- merchant and distinct upload: the same bytes again are the same row, and one merchant's upload never collides with
-- another's. A row only moves forward, once, from RECEIVED.
-- ---------------------------------------------------------------------------------------------------------------
CREATE TABLE settlement_uploads (
    merchant_id  text        NOT NULL REFERENCES merchants (merchant_id),
    sha256       text        NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    raw          bytea       NOT NULL CHECK (octet_length(raw) <= 16777216),
    received_at  timestamptz NOT NULL,
    state        text        NOT NULL CHECK (state IN ('RECEIVED', 'EMITTED', 'REFUSED_CONFLICT', 'REJECTED_MALFORMED')),
    error        text        CHECK (error IS NULL OR length(error) <= 300),
    processed_at timestamptz,
    CHECK ((state = 'RECEIVED') = (processed_at IS NULL)),
    CHECK ((state = 'REJECTED_MALFORMED') = (error IS NOT NULL)),
    PRIMARY KEY (merchant_id, sha256)
);
CREATE INDEX settlement_uploads_pending ON settlement_uploads (received_at, merchant_id, sha256)
    WHERE state = 'RECEIVED';

CREATE FUNCTION driftline_upload_forward_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state <> 'RECEIVED' THEN
        RAISE EXCEPTION 'driftline: settlement upload % is already %', OLD.sha256, OLD.state;
    END IF;
    IF NEW.sha256 <> OLD.sha256 OR NEW.merchant_id <> OLD.merchant_id OR NEW.raw <> OLD.raw
            OR NEW.received_at <> OLD.received_at THEN
        RAISE EXCEPTION 'driftline: a settlement upload''s bytes and receipt never change';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER settlement_uploads_forward_only BEFORE UPDATE ON settlement_uploads
    FOR EACH ROW EXECUTE FUNCTION driftline_upload_forward_only();
CREATE TRIGGER settlement_uploads_no_delete BEFORE DELETE ON settlement_uploads
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER settlement_uploads_no_truncate BEFORE TRUNCATE ON settlement_uploads
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();

-- ---------------------------------------------------------------------------------------------------------------
-- Quarantine of undecodable log records (§5.7, D47): the row keeps its position, a reason and the raw bytes; the
-- identity columns of a reused-key row stay required for that reason only.
-- ---------------------------------------------------------------------------------------------------------------
ALTER TABLE quarantine
    ALTER COLUMN merchant_id DROP NOT NULL,
    ALTER COLUMN source DROP NOT NULL,
    ALTER COLUMN event_key DROP NOT NULL,
    ALTER COLUMN payload_sha256 DROP NOT NULL,
    ADD COLUMN error text,
    ADD COLUMN raw bytea,
    DROP CONSTRAINT quarantine_reason_check,
    ADD CONSTRAINT quarantine_reason_check CHECK (reason IN ('IDEMPOTENCY_CONFLICT', 'UNDECODABLE')),
    ADD CONSTRAINT quarantine_shape CHECK (
        (reason = 'IDEMPOTENCY_CONFLICT' AND merchant_id IS NOT NULL AND source IS NOT NULL AND event_key IS NOT NULL
            AND payload_sha256 IS NOT NULL AND error IS NULL AND raw IS NULL)
        OR (reason = 'UNDECODABLE' AND merchant_id IS NULL AND source IS NULL AND event_key IS NULL
            AND payload_sha256 IS NULL AND length(error) BETWEEN 1 AND 300 AND octet_length(raw) <= 1048576));
CREATE INDEX quarantine_undecodable ON quarantine (partition, kafka_offset) WHERE reason = 'UNDECODABLE';

-- ---------------------------------------------------------------------------------------------------------------
-- Ownership and privileges. The application onboards merchants and issues tokens (INSERT), never edits or deletes
-- them; it moves an upload forward (UPDATE, guarded by the trigger).
-- ---------------------------------------------------------------------------------------------------------------
ALTER TABLE merchants OWNER TO driftline_migrator;
ALTER TABLE merchant_webhook_secrets OWNER TO driftline_migrator;
ALTER TABLE api_tokens OWNER TO driftline_migrator;
ALTER TABLE settlement_uploads OWNER TO driftline_migrator;

REVOKE ALL ON merchants, merchant_webhook_secrets, api_tokens, settlement_uploads FROM PUBLIC;
GRANT SELECT, INSERT ON merchants, merchant_webhook_secrets, api_tokens TO driftline_app;
GRANT SELECT, INSERT, UPDATE ON settlement_uploads TO driftline_app;
