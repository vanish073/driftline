-- Shift 11 (D99): customers' contact details are no longer kept forever. They are deleted once older than the
-- retention period (driftline.pii.retention), and on request (the ops API, scope pii:erase, audited). A deleted row
-- leaves a tombstone, which holds no personal data, so a later webhook for the same payment cannot store the contact
-- again. Rows still never change: the vault becomes delete-only instead of append-only.
DROP TRIGGER payment_customers_append_only ON payment_customers;
CREATE TRIGGER payment_customers_never_updated BEFORE UPDATE ON payment_customers
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
GRANT DELETE ON payment_customers TO driftline_app;

CREATE TABLE payment_customers_erased (
    merchant_id text        NOT NULL REFERENCES merchants (merchant_id),
    payment_id  text        NOT NULL CHECK (payment_id ~ '^[A-Za-z0-9_.-]{1,64}$'),
    erased_at   timestamptz NOT NULL,
    reason      text        NOT NULL CHECK (reason IN ('RETENTION', 'REQUEST')),
    PRIMARY KEY (merchant_id, payment_id)
);
CREATE TRIGGER payment_customers_erased_append_only BEFORE UPDATE OR DELETE ON payment_customers_erased
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE INDEX payment_customers_by_age ON payment_customers (first_seen_at);

ALTER TABLE payment_customers_erased OWNER TO driftline_migrator;
REVOKE ALL ON payment_customers_erased FROM PUBLIC;
GRANT SELECT, INSERT ON payment_customers_erased TO driftline_app;

-- pii:erase: erase a customer's stored contacts (ops tokens only, audited).
ALTER TABLE api_tokens DROP CONSTRAINT api_tokens_scopes_check;
ALTER TABLE api_tokens ADD CONSTRAINT api_tokens_scopes_check CHECK (cardinality(scopes) >= 1 AND scopes <@ ARRAY[
    'events:write', 'settlement:write', 'observations:write', 'breaks:read', 'breaks:resolve', 'ledger:adjust',
    'secrets:write', 'tokens:write', 'pii:erase']::text[]);
