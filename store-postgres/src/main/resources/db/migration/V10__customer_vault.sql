-- Shift 9 (D77, DESIGN §14): the customer's contact details from gateway webhooks, the only personal data Driftline
-- stores. Each field is AES-256-GCM ciphertext with its key id inside (written by ingest, which holds the key); the VPA
-- also has an HMAC blind index, keyed per merchant, for lookups. No plaintext column exists. Rows are written once per
-- payment and never changed.
CREATE TABLE payment_customers (
    merchant_id   text        NOT NULL REFERENCES merchants (merchant_id),
    payment_id    text        NOT NULL CHECK (payment_id ~ '^[A-Za-z0-9_.-]{1,64}$'),
    vpa_enc       bytea       CHECK (octet_length(vpa_enc) BETWEEN 29 AND 2048),
    phone_enc     bytea       CHECK (octet_length(phone_enc) BETWEEN 29 AND 2048),
    email_enc     bytea       CHECK (octet_length(email_enc) BETWEEN 29 AND 2048),
    vpa_index     text        CHECK (vpa_index ~ '^[0-9a-f]{64}$'),
    first_seen_at timestamptz NOT NULL,
    PRIMARY KEY (merchant_id, payment_id),
    CHECK (vpa_enc IS NOT NULL OR phone_enc IS NOT NULL OR email_enc IS NOT NULL),
    CHECK ((vpa_enc IS NULL) = (vpa_index IS NULL))
);
CREATE INDEX payment_customers_by_vpa ON payment_customers (merchant_id, vpa_index) WHERE vpa_index IS NOT NULL;

CREATE TRIGGER payment_customers_append_only BEFORE UPDATE OR DELETE ON payment_customers
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();

ALTER TABLE payment_customers OWNER TO driftline_migrator;
REVOKE ALL ON payment_customers FROM PUBLIC;
GRANT SELECT, INSERT ON payment_customers TO driftline_app;
