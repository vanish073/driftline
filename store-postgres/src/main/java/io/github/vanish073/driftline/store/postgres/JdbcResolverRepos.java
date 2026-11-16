package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.OutboxRepo;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/** The outbox and the audit log on PostgreSQL (DESIGN §9, §7.6; V1, V7). */
final class JdbcResolverRepos {

    private JdbcResolverRepos() {
    }

    private static OffsetDateTime ts(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }

    private static Optional<Instant> optionalInstant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? Optional.empty() : Optional.of(t.toInstant());
    }

    /** A flat jsonb object of strings, read through two arrays sorted by key in C order (D45). */
    private static String mapColumns(String column) {
        return "ARRAY(SELECT key FROM jsonb_each_text(" + column + ") ORDER BY key COLLATE \"C\") AS " + column
                + "_keys, ARRAY(SELECT value FROM jsonb_each_text(" + column + ") ORDER BY key COLLATE \"C\") AS "
                + column + "_values";
    }

    private static SortedMap<String, String> map(ResultSet rs, String column) throws SQLException {
        TreeMap<String, String> out = new TreeMap<>();
        String[] keys = (String[]) rs.getArray(column + "_keys").getArray();
        String[] values = (String[]) rs.getArray(column + "_values").getArray();
        for (int i = 0; i < keys.length; i++) {
            out.put(keys[i], values[i]);
        }
        return out;
    }

    private static void setMap(Connection c, PreparedStatement ps, int keysAt, Map<String, String> map)
            throws SQLException {
        TreeMap<String, String> sorted = new TreeMap<>(map);
        Array keys = c.createArrayOf("text", sorted.keySet().toArray(new String[0]));
        Array values = c.createArrayOf("text", sorted.values().toArray(new String[0]));
        ps.setArray(keysAt, keys);
        ps.setArray(keysAt + 1, values);
    }

    // ---------------------------------------------------------------------------------------------------------------

    static final class Outbox implements OutboxRepo {
        private static final String SELECT = "SELECT command_id, merchant_id, break_id, kind, idempotency_key, status, "
                + "attempt, lease_owner, lease_until, next_attempt_at, first_sent_at, (result IS NOT NULL) AS has_result, "
                + mapColumns("args") + ", " + mapColumns("result") + " FROM outbox";

        private final JdbcTx tx;

        Outbox(JdbcTx tx) {
            this.tx = tx;
        }

        private static Command read(ResultSet rs) throws SQLException {
            Optional<SortedMap<String, String>> result = rs.getBoolean("has_result")
                    ? Optional.of(map(rs, "result")) : Optional.empty();
            return new Command(rs.getString("command_id"), MerchantId.of(rs.getString("merchant_id")),
                    rs.getString("break_id"), CommandKind.valueOf(rs.getString("kind")), map(rs, "args"),
                    Optional.ofNullable(rs.getString("idempotency_key")), CommandStatus.valueOf(rs.getString("status")),
                    rs.getInt("attempt"), Optional.ofNullable(rs.getString("lease_owner")),
                    optionalInstant(rs, "lease_until"), instant(rs, "next_attempt_at"),
                    optionalInstant(rs, "first_sent_at"), result);
        }

        private List<Command> list(String sql, Object... params) {
            try (PreparedStatement ps = tx.connection().prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
                List<Command> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(read(rs));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("outbox", e);
            }
        }

        @Override
        public boolean enqueue(Command command) {
            if (command.status() != CommandStatus.PENDING || command.attempt() != 0 || command.leaseOwner().isPresent()) {
                throw new IllegalArgumentException("a new command is PENDING, unclaimed and unleased");
            }
            if (tx.breaks().byId(command.breakId()).filter(b -> b.merchantId().equals(command.merchantId())).isEmpty()) {
                throw new IllegalArgumentException("no break " + command.breakId());
            }
            Connection c = tx.connection();
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO outbox (command_id, merchant_id, break_id, kind, args, idempotency_key, status, attempt,
                                        next_attempt_at)
                    VALUES (?, ?, ?, ?, jsonb_object(?::text[], ?::text[]), ?, 'PENDING', 0, ?)
                    ON CONFLICT (command_id) DO NOTHING
                    """)) {
                ps.setString(1, command.commandId());
                ps.setString(2, command.merchantId().value());
                ps.setString(3, command.breakId());
                ps.setString(4, command.kind().name());
                setMap(c, ps, 5, command.args());
                ps.setString(7, command.idempotencyKey().orElse(null));
                ps.setObject(8, ts(command.nextAttemptAt()));
                return ps.executeUpdate() == 1;
            } catch (SQLException e) {
                throw SqlErrors.translate("enqueue " + command.commandId(), e);
            }
        }

        @Override
        public Optional<Command> find(String commandId) {
            return list(SELECT + " WHERE command_id = ?", commandId).stream().findFirst();
        }

        @Override
        public List<Command> forBreak(MerchantId merchantId, String breakId) {
            return list(SELECT + " WHERE merchant_id = ? AND break_id = ? ORDER BY command_id COLLATE \"C\"",
                    merchantId.value(), breakId);
        }

        @Override
        public List<Command> claim(Instant now, String owner, Instant leaseUntil, Instant retryAt, int limit) {
            List<Command> due = list(SELECT + """
                     WHERE status <> 'DONE' AND next_attempt_at <= ? AND (lease_until IS NULL OR lease_until < ?)
                     ORDER BY next_attempt_at, command_id COLLATE "C"
                     LIMIT ? FOR UPDATE SKIP LOCKED
                    """, ts(now), ts(now), limit);
            List<Command> out = new ArrayList<>();
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    UPDATE outbox SET status = 'IN_FLIGHT', lease_owner = ?, lease_until = ?, attempt = attempt + 1,
                                      first_sent_at = coalesce(first_sent_at, ?), next_attempt_at = ?
                     WHERE command_id = ?
                    """)) {
                for (Command c : due) {
                    ps.setString(1, owner);
                    ps.setObject(2, ts(leaseUntil));
                    ps.setObject(3, ts(now));
                    ps.setObject(4, ts(retryAt));
                    ps.setString(5, c.commandId());
                    ps.executeUpdate();
                    out.add(find(c.commandId()).orElseThrow());
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("claim commands", e);
            }
            return List.copyOf(out);
        }

        @Override
        public boolean complete(String commandId, Map<String, String> result) {
            Command c = list(SELECT + " WHERE command_id = ? FOR UPDATE", commandId).stream().findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("no command " + commandId));
            if (c.status() == CommandStatus.DONE) {
                return false;
            }
            Connection conn = tx.connection();
            try (PreparedStatement ps = conn.prepareStatement("""
                    UPDATE outbox SET status = 'DONE', result = jsonb_object(?::text[], ?::text[]), lease_owner = NULL,
                                      lease_until = NULL
                     WHERE command_id = ?
                    """)) {
                setMap(conn, ps, 1, result);
                ps.setString(3, commandId);
                ps.executeUpdate();
                return true;
            } catch (SQLException e) {
                throw SqlErrors.translate("complete " + commandId, e);
            }
        }

        @Override
        public void retryAt(String commandId, Instant nextAttemptAt) {
            Command c = list(SELECT + " WHERE command_id = ? FOR UPDATE", commandId).stream().findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("no command " + commandId));
            if (c.status() == CommandStatus.DONE) {
                return;
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    UPDATE outbox SET status = 'PENDING', lease_owner = NULL, lease_until = NULL, next_attempt_at = ?
                     WHERE command_id = ?
                    """)) {
                ps.setObject(1, ts(nextAttemptAt));
                ps.setString(2, commandId);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("retry " + commandId, e);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    static final class Audit implements AuditRepo {
        private static final String SELECT = "SELECT seq, at, actor, action, subject, prev_hash, hash, "
                + mapColumns("details") + " FROM audit_log";

        private final JdbcTx tx;

        Audit(JdbcTx tx) {
            this.tx = tx;
        }

        @Override
        public Row append(Entry entry) {
            Connection c = tx.connection();
            try {
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT pg_advisory_xact_lock(hashtext('driftline_audit_log'))")) {
                    ps.execute();
                }
                String prev = GENESIS;
                try (PreparedStatement ps = c.prepareStatement("SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1");
                     ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        prev = rs.getString(1);
                    }
                }
                String hash = AuditRepo.hash(prev, entry);
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO audit_log (at, actor, action, subject, details, prev_hash, hash)
                        VALUES (?, ?, ?, ?, jsonb_object(?::text[], ?::text[]), ?, ?)
                        RETURNING seq
                        """)) {
                    ps.setObject(1, ts(entry.at()));
                    ps.setString(2, entry.actor());
                    ps.setString(3, entry.action());
                    ps.setString(4, entry.subject());
                    setMap(c, ps, 5, entry.details());
                    ps.setString(7, prev);
                    ps.setString(8, hash);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return new Row(rs.getLong(1), entry, prev, hash);
                    }
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("append to the audit log", e);
            }
        }

        @Override
        public List<Row> rows() {
            try (PreparedStatement ps = tx.connection().prepareStatement(SELECT + " ORDER BY seq");
                 ResultSet rs = ps.executeQuery()) {
                List<Row> out = new ArrayList<>();
                while (rs.next()) {
                    Entry e = new Entry(instant(rs, "at"), rs.getString("actor"), rs.getString("action"),
                            rs.getString("subject"), map(rs, "details"));
                    out.add(new Row(rs.getLong("seq"), e, rs.getString("prev_hash"), rs.getString("hash")));
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("the audit log", e);
            }
        }
    }
}
