-- Shift 9 (D78, DESIGN §13): the W3C trace context under which a break was raised or an outbox command was enqueued,
-- so the resolver and the dispatcher, which may run in other processes, continue the trace that began at ingest.
-- Written by the app's drivers after their own transaction commits (best effort: a lost row loses a trace, nothing
-- else), only when a trace is being recorded. The core never reads it. Rows are written once and never changed.
CREATE TABLE trace_contexts (
    subject     text        PRIMARY KEY CHECK (subject ~ '^(brk|cmd)_[A-Za-z0-9_.-]{1,64}$'),
    traceparent text        NOT NULL CHECK (traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'),
    created_at  timestamptz NOT NULL
);

CREATE TRIGGER trace_contexts_append_only BEFORE UPDATE OR DELETE ON trace_contexts
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();

ALTER TABLE trace_contexts OWNER TO driftline_migrator;
REVOKE ALL ON trace_contexts FROM PUBLIC;
GRANT SELECT, INSERT ON trace_contexts TO driftline_app;
