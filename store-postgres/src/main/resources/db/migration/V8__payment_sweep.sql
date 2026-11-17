-- Shift 8 (D72, BUG-010): PAYMENT_SWEEP, the gateway's list of one day's captures, asked before a file's unreferenced
-- lines are solved. Like the pending-order probe, a sweep belongs to no break, so its break_id is NULL.
ALTER TABLE outbox DROP CONSTRAINT outbox_kind_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_kind_check CHECK (kind IN ('STATUS_QUERY_PAYMENT', 'STATUS_QUERY_ORDER',
    'REFUND', 'REFUND_LOOKUP', 'MERCHANT_CONFIRM', 'PUBLISH_OBSERVATION', 'ESCALATION_NOTIFY', 'PAYMENT_SWEEP'));
