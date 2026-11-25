-- Shift 11 (D95): API tokens expire, can be revoked through the API, and have a public id. Before this, tokens never
-- expired and could be revoked only by editing the database.
--
-- token_id   'tok_' and the first 16 hex digits of the token's SHA-256: names a token in the API, the audit trail and
--            logs without revealing anything that helps to forge it.
-- expires_at every token expires; the API caps the lifetime (90 days for ops and service tokens, 365 for merchant
--            tokens). Tokens issued before this migration get 90 days from their creation.
-- created_by the actor that created it ('bootstrap' for the dev bootstrap file, 'migration' for older rows).
ALTER TABLE api_tokens ADD COLUMN token_id text, ADD COLUMN expires_at timestamptz, ADD COLUMN created_by text;
UPDATE api_tokens SET token_id = 'tok_' || left(token_sha256, 16), expires_at = created_at + interval '90 days',
                      created_by = 'migration';
ALTER TABLE api_tokens ALTER COLUMN token_id SET NOT NULL, ALTER COLUMN expires_at SET NOT NULL,
                       ALTER COLUMN created_by SET NOT NULL;
ALTER TABLE api_tokens ADD CONSTRAINT api_tokens_token_id_key UNIQUE (token_id),
    ADD CONSTRAINT api_tokens_token_id_check CHECK (token_id = 'tok_' || left(token_sha256, 16)),
    ADD CONSTRAINT api_tokens_expiry_check CHECK (expires_at > created_at),
    ADD CONSTRAINT api_tokens_created_by_check CHECK (length(created_by) BETWEEN 1 AND 255);

-- tokens:write: create, list and revoke tokens. A token can only grant scopes its holder has (checked by the API).
ALTER TABLE api_tokens DROP CONSTRAINT api_tokens_scopes_check;
ALTER TABLE api_tokens ADD CONSTRAINT api_tokens_scopes_check CHECK (cardinality(scopes) >= 1 AND scopes <@ ARRAY[
    'events:write', 'settlement:write', 'observations:write', 'breaks:read', 'breaks:resolve', 'ledger:adjust',
    'secrets:write', 'tokens:write']::text[]);

-- Revocation through the API: the application may set revoked_at, and nothing else. A revoked token stays revoked,
-- and no other column of a token ever changes, whoever runs the UPDATE (the owner included); rows are never deleted.
CREATE FUNCTION driftline_token_revoke_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'api_tokens rows are never deleted; revoke the token instead' USING ERRCODE = 'check_violation';
    END IF;
    IF OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
       OR NEW.token_sha256 IS DISTINCT FROM OLD.token_sha256 OR NEW.token_id IS DISTINCT FROM OLD.token_id
       OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id OR NEW.scopes IS DISTINCT FROM OLD.scopes
       OR NEW.created_at IS DISTINCT FROM OLD.created_at OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
       OR NEW.created_by IS DISTINCT FROM OLD.created_by THEN
        RAISE EXCEPTION 'an api token can only be revoked, once' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER api_tokens_revoke_only BEFORE UPDATE OR DELETE ON api_tokens
    FOR EACH ROW EXECUTE FUNCTION driftline_token_revoke_only();

GRANT UPDATE (revoked_at) ON api_tokens TO driftline_app;
