-- Shift 9: two ops scopes and the one privilege rotation needs.
-- ledger:adjust (D74): the audited manual-adjustment API (DESIGN §7, D8). Moving money in the books is a stronger power
-- than closing a break (breaks:resolve), so it has a scope of its own.
-- secrets:write (D76): rotating a merchant's webhook secret, audited.
ALTER TABLE api_tokens DROP CONSTRAINT api_tokens_scopes_check;
ALTER TABLE api_tokens ADD CONSTRAINT api_tokens_scopes_check CHECK (cardinality(scopes) >= 1 AND scopes <@ ARRAY[
    'events:write', 'settlement:write', 'observations:write', 'breaks:read', 'breaks:resolve', 'ledger:adjust',
    'secrets:write']::text[]);

-- A rotation closes the old secret's window (active_to); the application may change that column and nothing else,
-- so a stored secret itself can never be rewritten, only superseded.
GRANT UPDATE (active_to) ON merchant_webhook_secrets TO driftline_app;
