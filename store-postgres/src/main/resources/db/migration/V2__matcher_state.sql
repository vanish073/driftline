-- Driftline V2: the matcher's state (DESIGN §3.2, §5, §12): dedupe rows, payments, orders, their transition logs,
-- and consumer offsets per partition. The partition owner and fencing epoch are used from Shift 4 (§5.1).

CREATE TABLE processed_events (
    merchant_id    text   NOT NULL,
    source         text   NOT NULL CHECK (source IN ('GATEWAY', 'MERCHANT', 'BANK', 'OBSERVATION')),
    event_key      text   NOT NULL,
    payload_sha256 text   NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    partition      int    NOT NULL,
    kafka_offset   bigint NOT NULL,
    PRIMARY KEY (merchant_id, source, event_key)
);

CREATE TABLE payments (
    merchant_id  text    NOT NULL,
    payment_id   text    NOT NULL,
    status       text    NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    order_id     text    NOT NULL,
    amount_paise bigint  NOT NULL CHECK (amount_paise > 0),
    failed_at    timestamptz,
    success_at   timestamptz,
    conflict     boolean NOT NULL DEFAULT false,
    PRIMARY KEY (merchant_id, payment_id),
    CHECK (failed_at IS NULL OR success_at IS NULL OR failed_at < success_at)
);

CREATE TABLE payment_transitions (
    merchant_id text        NOT NULL,
    payment_id  text        NOT NULL,
    seq         int         NOT NULL,
    from_status text        NOT NULL,
    to_status   text        NOT NULL,
    rule        text        NOT NULL,
    event_key   text        NOT NULL,
    event_time  timestamptz NOT NULL,
    PRIMARY KEY (merchant_id, payment_id, seq)
);

CREATE TABLE orders (
    merchant_id  text   NOT NULL,
    order_id     text   NOT NULL,
    status       text   NOT NULL CHECK (status IN ('CREATED', 'PENDING_PAYMENT', 'PAID', 'FULFILLED', 'EXPIRED',
                                                    'CANCELLED')),
    version      bigint NOT NULL CHECK (version >= 1),
    amount_paise bigint NOT NULL CHECK (amount_paise > 0),
    expires_at   timestamptz NOT NULL,
    paid_with    text,
    PRIMARY KEY (merchant_id, order_id),
    CHECK ((paid_with IS NOT NULL) = (status IN ('PAID', 'FULFILLED')))
);

CREATE TABLE order_transitions (
    merchant_id text        NOT NULL,
    order_id    text        NOT NULL,
    seq         int         NOT NULL,
    from_status text        NOT NULL,
    to_status   text        NOT NULL,
    version     bigint      NOT NULL,
    rule        text        NOT NULL,
    event_key   text        NOT NULL,
    event_time  timestamptz NOT NULL,
    PRIMARY KEY (merchant_id, order_id, seq)
);

CREATE TABLE partition_leases (
    partition   int    PRIMARY KEY CHECK (partition >= 0),
    epoch       bigint NOT NULL DEFAULT 0,
    owner       text,
    next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0)
);

-- Dedupe rows and transition logs are append-only, like the ledger.
CREATE TRIGGER processed_events_append_only BEFORE UPDATE OR DELETE ON processed_events
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER processed_events_no_truncate BEFORE TRUNCATE ON processed_events
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER payment_transitions_append_only BEFORE UPDATE OR DELETE ON payment_transitions
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER payment_transitions_no_truncate BEFORE TRUNCATE ON payment_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER order_transitions_append_only BEFORE UPDATE OR DELETE ON order_transitions
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER order_transitions_no_truncate BEFORE TRUNCATE ON order_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();

ALTER TABLE processed_events OWNER TO driftline_migrator;
ALTER TABLE payments OWNER TO driftline_migrator;
ALTER TABLE payment_transitions OWNER TO driftline_migrator;
ALTER TABLE orders OWNER TO driftline_migrator;
ALTER TABLE order_transitions OWNER TO driftline_migrator;
ALTER TABLE partition_leases OWNER TO driftline_migrator;

REVOKE ALL ON processed_events, payments, payment_transitions, orders, order_transitions, partition_leases FROM PUBLIC;
GRANT SELECT, INSERT ON processed_events, payment_transitions, order_transitions TO driftline_app;
GRANT SELECT, INSERT, UPDATE ON payments, orders, partition_leases TO driftline_app;
