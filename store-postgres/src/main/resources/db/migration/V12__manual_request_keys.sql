-- Shift 11 (D94): repeat-safe manual actions. A manual action (a ledger adjustment, a manual resolution, a webhook
-- secret rotation, a token or erasure call) records the caller's Idempotency-Key in its own audit row's details, in the
-- same transaction as the action. This index makes the key unique per action and subject, so two concurrent repeats of
-- one request cannot both commit (the second fails with a unique violation, and its retry finds the first). Rows from
-- before D94 have no request_key and are not indexed. The audit log stays append-only; nothing else changes.
CREATE UNIQUE INDEX audit_log_request_key ON audit_log (action, subject, (details->>'request_key'))
    WHERE details ? 'request_key';
