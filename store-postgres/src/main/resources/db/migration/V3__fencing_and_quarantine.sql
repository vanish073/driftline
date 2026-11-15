-- Driftline V3 (Shift 4): partition epoch fencing (DESIGN §5.1) and the quarantine of refused log records (§5.7, §10.3).

-- A lease never goes back: neither its epoch nor its offset may decrease. The store checks both before writing;
-- this trigger is the database's own guard, so raw SQL cannot un-fence a zombie either.
CREATE FUNCTION driftline_lease_forward_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.partition <> OLD.partition THEN
        RAISE EXCEPTION 'partition_leases: the partition of a lease never changes' USING ERRCODE = 'P0001';
    END IF;
    IF NEW.epoch < OLD.epoch THEN
        RAISE EXCEPTION 'partition_leases: epoch of partition % cannot go back from % to %',
            OLD.partition, OLD.epoch, NEW.epoch USING ERRCODE = 'P0001';
    END IF;
    IF NEW.next_offset < OLD.next_offset THEN
        RAISE EXCEPTION 'partition_leases: offset of partition % cannot go back from % to %',
            OLD.partition, OLD.next_offset, NEW.next_offset USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER partition_leases_forward_only BEFORE UPDATE ON partition_leases
    FOR EACH ROW EXECUTE FUNCTION driftline_lease_forward_only();

-- One row per refused log record. The id is derived from (partition, offset), so a replayed batch rewrites the same
-- row. The raw bytes of an undecodable record arrive with the Kafka driver (Shift 6); a key reused with a different
-- body is identified by its key and payload hash.
CREATE TABLE quarantine (
    id             text   PRIMARY KEY,
    partition      int    NOT NULL CHECK (partition >= 0),
    kafka_offset   bigint NOT NULL CHECK (kafka_offset >= 0),
    merchant_id    text   NOT NULL,
    source         text   NOT NULL CHECK (source IN ('GATEWAY', 'MERCHANT', 'BANK', 'OBSERVATION')),
    event_key      text   NOT NULL,
    payload_sha256 text   NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    reason         text   NOT NULL CHECK (reason IN ('IDEMPOTENCY_CONFLICT')),
    UNIQUE (partition, kafka_offset)
);
CREATE INDEX quarantine_by_merchant ON quarantine (merchant_id, partition, kafka_offset);

CREATE TRIGGER quarantine_append_only BEFORE UPDATE OR DELETE ON quarantine
    FOR EACH ROW EXECUTE FUNCTION driftline_append_only();
CREATE TRIGGER quarantine_no_truncate BEFORE TRUNCATE ON quarantine
    FOR EACH STATEMENT EXECUTE FUNCTION driftline_append_only();

ALTER TABLE quarantine OWNER TO driftline_migrator;
REVOKE ALL ON quarantine FROM PUBLIC;
GRANT SELECT, INSERT ON quarantine TO driftline_app;
