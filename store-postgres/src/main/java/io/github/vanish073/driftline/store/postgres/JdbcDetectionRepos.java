package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.TimerRepo;
import io.github.vanish073.driftline.core.port.WatermarkRepo;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The watermark, timer, break and settlement repositories on PostgreSQL (migration V4). Each check the in-memory store
 * makes in Java is made here in Java too, under a row lock, so both stores refuse the same calls with the same
 * exception; the database triggers are a second guard against raw SQL. Every ORDER BY on text uses
 * {@code COLLATE "C"}, which sorts as Java's {@code String.compareTo} does.
 */
final class JdbcDetectionRepos {

    private JdbcDetectionRepos() {
    }

    private static OffsetDateTime ts(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }

    private static Optional<Instant> optionalInstant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? Optional.empty() : Optional.of(value.toInstant());
    }

    private static void setOptionalTs(PreparedStatement ps, int index, Optional<Instant> value) throws SQLException {
        if (value.isPresent()) {
            ps.setObject(index, ts(value.get()));
        } else {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        }
    }

    private static void setOptionalMoney(PreparedStatement ps, int index, Optional<Money> value) throws SQLException {
        if (value.isPresent()) {
            ps.setLong(index, value.get().paise());
        } else {
            ps.setNull(index, Types.BIGINT);
        }
    }

    private static Optional<Money> optionalMoney(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? Optional.empty() : Optional.of(Money.ofPaise(value));
    }

    private static void setOptionalText(PreparedStatement ps, int index, Optional<String> value) throws SQLException {
        if (value.isPresent()) {
            ps.setString(index, value.get());
        } else {
            ps.setNull(index, Types.VARCHAR);
        }
    }

    private static LocalDate date(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, LocalDate.class);
    }

    // ---------------------------------------------------------------------------------------------------------------

    static final class Watermarks implements WatermarkRepo {
        private final JdbcTx tx;

        Watermarks(JdbcTx tx) {
            this.tx = tx;
        }

        private Optional<StreamWatermark> read(int partition, Source source, boolean lock) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT value, max_event_time, last_event_at FROM watermarks WHERE partition = ? AND source = ?"
                            + (lock ? " FOR UPDATE" : ""))) {
                ps.setInt(1, partition);
                ps.setString(2, source.name());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new StreamWatermark(partition, source, instant(rs, "value"),
                            optionalInstant(rs, "max_event_time"), optionalInstant(rs, "last_event_at")));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("watermark p" + partition + "/" + source, e);
            }
        }

        @Override
        public Optional<StreamWatermark> stream(int partition, Source source) {
            return read(partition, source, false);
        }

        @Override
        public void saveStream(StreamWatermark w) {
            Optional<StreamWatermark> current = read(w.partition(), w.source(), true);
            if (current.isPresent()) {
                StreamWatermark c = current.get();
                if (w.value().isBefore(c.value())) {
                    throw new IllegalArgumentException("watermark of p" + w.partition() + "/" + w.source()
                            + " cannot move back");
                }
                if (c.maxEventTime().isPresent()
                        && (w.maxEventTime().isEmpty() || w.maxEventTime().get().isBefore(c.maxEventTime().get()))) {
                    throw new IllegalArgumentException("max event time of p" + w.partition() + "/" + w.source()
                            + " cannot move back");
                }
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO watermarks (partition, source, value, max_event_time, last_event_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (partition, source) DO UPDATE SET value = EXCLUDED.value,
                        max_event_time = EXCLUDED.max_event_time, last_event_at = EXCLUDED.last_event_at
                    """)) {
                ps.setInt(1, w.partition());
                ps.setString(2, w.source().name());
                ps.setObject(3, ts(w.value()));
                setOptionalTs(ps, 4, w.maxEventTime());
                setOptionalTs(ps, 5, w.lastEventAt());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save watermark p" + w.partition() + "/" + w.source(), e);
            }
        }

        private Optional<LocalDate> settled(MerchantId merchantId, boolean lock) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT complete_through FROM settlement_watermarks WHERE merchant_id = ?" + (lock ? " FOR UPDATE" : ""))) {
                ps.setString(1, merchantId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(date(rs, "complete_through")) : Optional.empty();
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("settlement watermark of " + merchantId, e);
            }
        }

        @Override
        public Optional<LocalDate> settledThrough(MerchantId merchantId) {
            return settled(merchantId, false);
        }

        @Override
        public void saveSettledThrough(MerchantId merchantId, LocalDate date) {
            Optional<LocalDate> current = settled(merchantId, true);
            if (current.isPresent() && date.isBefore(current.get())) {
                throw new IllegalArgumentException("settlement watermark of " + merchantId + " cannot move back");
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO settlement_watermarks (merchant_id, complete_through) VALUES (?, ?)
                    ON CONFLICT (merchant_id) DO UPDATE SET complete_through = EXCLUDED.complete_through
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setObject(2, date);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save settlement watermark of " + merchantId, e);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    static final class Timers implements TimerRepo {
        private static final String COLUMNS = "merchant_id, kind, subject_key, partition, clock, fire_at";
        private static final String FIRING_ORDER =
                " ORDER BY fire_at, kind COLLATE \"C\", subject_key COLLATE \"C\", merchant_id COLLATE \"C\"";

        private final JdbcTx tx;

        Timers(JdbcTx tx) {
            this.tx = tx;
        }

        private static Timer timer(ResultSet rs) throws SQLException {
            return new Timer(MerchantId.of(rs.getString("merchant_id")), Kind.valueOf(rs.getString("kind")),
                    rs.getString("subject_key"), rs.getInt("partition"), TimerClock.valueOf(rs.getString("clock")),
                    instant(rs, "fire_at"));
        }

        private List<Timer> list(String sql, Object... params) {
            try (PreparedStatement ps = tx.connection().prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
                List<Timer> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(timer(rs));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("timers", e);
            }
        }

        @Override
        public void upsert(Timer t) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO timers (merchant_id, kind, subject_key, partition, clock, fire_at) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, kind, subject_key) DO UPDATE SET partition = EXCLUDED.partition,
                        clock = EXCLUDED.clock, fire_at = EXCLUDED.fire_at
                    """)) {
                ps.setString(1, t.merchantId().value());
                ps.setString(2, t.kind().name());
                ps.setString(3, t.subjectKey());
                ps.setInt(4, t.partition());
                ps.setString(5, t.clock().name());
                ps.setObject(6, ts(t.fireAt()));
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("upsert timer " + t.kind() + " " + t.subjectKey(), e);
            }
        }

        @Override
        public Optional<Timer> find(MerchantId merchantId, Kind kind, String subjectKey) {
            List<Timer> found = list("SELECT " + COLUMNS + " FROM timers WHERE merchant_id = ? AND kind = ? AND subject_key = ?",
                    merchantId.value(), kind.name(), subjectKey);
            return found.stream().findFirst();
        }

        @Override
        public List<Timer> due(int partition, TimerClock clock, Instant upTo) {
            return list("SELECT " + COLUMNS + " FROM timers WHERE partition = ? AND clock = ? AND fire_at <= ?"
                    + FIRING_ORDER, partition, clock.name(), ts(upTo));
        }

        @Override
        public List<Timer> dueForMerchant(MerchantId merchantId, TimerClock clock, Instant upTo) {
            return list("SELECT " + COLUMNS + " FROM timers WHERE merchant_id = ? AND clock = ? AND fire_at <= ?"
                    + FIRING_ORDER, merchantId.value(), clock.name(), ts(upTo));
        }

        @Override
        public Optional<Instant> earliest(int partition, TimerClock clock) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT min(fire_at) FROM timers WHERE partition = ? AND clock = ?")) {
                ps.setInt(1, partition);
                ps.setString(2, clock.name());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    OffsetDateTime min = rs.getObject(1, OffsetDateTime.class);
                    return min == null ? Optional.empty() : Optional.of(min.toInstant());
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("earliest timer p" + partition, e);
            }
        }

        @Override
        public void delete(MerchantId merchantId, Kind kind, String subjectKey) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "DELETE FROM timers WHERE merchant_id = ? AND kind = ? AND subject_key = ?")) {
                ps.setString(1, merchantId.value());
                ps.setString(2, kind.name());
                ps.setString(3, subjectKey);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("delete timer " + kind + " " + subjectKey, e);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    static final class Breaks implements BreakRepo {
        private static final String SELECT = """
                SELECT break_id, merchant_id, type, subject_key, state, detected_at, version,
                       ARRAY(SELECT key FROM jsonb_each_text(evidence) ORDER BY key COLLATE "C") AS evidence_keys,
                       ARRAY(SELECT value FROM jsonb_each_text(evidence) ORDER BY key COLLATE "C") AS evidence_values
                  FROM breaks
                """;

        private final JdbcTx tx;

        Breaks(JdbcTx tx) {
            this.tx = tx;
        }

        private static Break read(ResultSet rs) throws SQLException {
            TreeMap<String, String> evidence = new TreeMap<>();
            String[] keys = (String[]) rs.getArray("evidence_keys").getArray();
            String[] values = (String[]) rs.getArray("evidence_values").getArray();
            for (int i = 0; i < keys.length; i++) {
                evidence.put(keys[i], values[i]);
            }
            return new Break(rs.getString("break_id"), MerchantId.of(rs.getString("merchant_id")),
                    BreakType.valueOf(rs.getString("type")), rs.getString("subject_key"),
                    BreakState.valueOf(rs.getString("state")), instant(rs, "detected_at"), evidence,
                    rs.getLong("version"));
        }

        private List<Break> list(String sql, String... params) {
            try (PreparedStatement ps = tx.connection().prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setString(i + 1, params[i]);
                }
                List<Break> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(read(rs));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("breaks", e);
            }
        }

        private Optional<Break> byId(MerchantId merchantId, String breakId, boolean lock) {
            return list(SELECT + " WHERE merchant_id = ? AND break_id = ?" + (lock ? " FOR UPDATE" : ""),
                    merchantId.value(), breakId).stream().findFirst();
        }

        @Override
        public Raised raise(Break b, String reason) {
            Connection c = tx.connection();
            try {
                Array keys = c.createArrayOf("text", b.evidence().keySet().toArray(new String[0]));
                Array values = c.createArrayOf("text", b.evidence().values().toArray(new String[0]));
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO breaks (break_id, merchant_id, kind, type, subject_key, state, detected_at, evidence,
                                            version)
                        VALUES (?, ?, ?, ?, ?, ?, ?, jsonb_object(?::text[], ?::text[]), ?)
                        ON CONFLICT (break_id) DO NOTHING
                        """)) {
                    ps.setString(1, b.breakId());
                    ps.setString(2, b.merchantId().value());
                    ps.setString(3, b.kind().name());
                    ps.setString(4, b.type().name());
                    ps.setString(5, b.subjectKey());
                    ps.setString(6, b.state().name());
                    ps.setObject(7, ts(b.detectedAt()));
                    ps.setArray(8, keys);
                    ps.setArray(9, values);
                    ps.setLong(10, b.version());
                    if (ps.executeUpdate() == 0) {
                        return new Raised(false, byId(b.merchantId(), b.breakId(), false).orElseThrow(
                                () -> new IllegalStateException("break " + b.breakId() + " conflicted but cannot be read")));
                    }
                }
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO break_transitions (break_id, seq, from_state, to_state, at, reason)
                        VALUES (?, 1, NULL, ?, ?, ?)
                        """)) {
                    ps.setString(1, b.breakId());
                    ps.setString(2, b.state().name());
                    ps.setObject(3, ts(b.detectedAt()));
                    ps.setString(4, reason);
                    ps.executeUpdate();
                }
                return new Raised(true, b);
            } catch (SQLException e) {
                throw SqlErrors.translate("raise break " + b.type() + " " + b.subjectKey(), e);
            }
        }

        @Override
        public Optional<Break> find(MerchantId merchantId, BreakType type, String subjectKey) {
            return byId(merchantId, Break.idOf(merchantId, type, subjectKey), false);
        }

        @Override
        public void addEvidence(MerchantId merchantId, String breakId, String key, String value) {
            Break.requireEvidence(key, value);
            Break b = byId(merchantId, breakId, true)
                    .orElseThrow(() -> new IllegalArgumentException("no break " + breakId));
            if (value.equals(b.evidence().get(key))) {
                return;
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    UPDATE breaks SET evidence = evidence || jsonb_build_object(?::text, ?::text), version = version + 1
                     WHERE merchant_id = ? AND break_id = ?
                    """)) {
                ps.setString(1, key);
                ps.setString(2, value);
                ps.setString(3, merchantId.value());
                ps.setString(4, breakId);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("add evidence to " + breakId, e);
            }
        }

        @Override
        public void transition(MerchantId merchantId, String breakId, BreakState to, Instant at, String reason) {
            Break.requireEvidence("reason", reason);
            Break b = byId(merchantId, breakId, true)
                    .orElseThrow(() -> new IllegalArgumentException("no break " + breakId));
            if (b.state() == to) {
                throw new IllegalArgumentException("break " + breakId + " is already " + to);
            }
            Connection c = tx.connection();
            try {
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO break_transitions (break_id, seq, from_state, to_state, at, reason)
                        SELECT ?, coalesce(max(seq), 0) + 1, ?, ?, ?, ? FROM break_transitions WHERE break_id = ?
                        """)) {
                    ps.setString(1, breakId);
                    ps.setString(2, b.state().name());
                    ps.setString(3, to.name());
                    ps.setObject(4, ts(at));
                    ps.setString(5, reason);
                    ps.setString(6, breakId);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE breaks SET state = ?, version = version + 1 WHERE merchant_id = ? AND break_id = ?")) {
                    ps.setString(1, to.name());
                    ps.setString(2, merchantId.value());
                    ps.setString(3, breakId);
                    ps.executeUpdate();
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("transition " + breakId, e);
            }
        }

        // ---- the resolver's plan (V7) ----

        private static final String PLAN = """
                SELECT rule_id, deadline_at, escalate_by, review_at, action, resolved_at, compensation_paise
                  FROM breaks WHERE merchant_id = ? AND break_id = ?
                """;

        @Override
        public void savePlan(MerchantId merchantId, String breakId, Plan plan) {
            Optional<Plan> existing = planOf(merchantId, breakId);
            if (existing.isEmpty() && byId(merchantId, breakId, false).isEmpty()) {
                throw new IllegalArgumentException("no break " + breakId);
            }
            if (existing.flatMap(Plan::action).isPresent() && !existing.get().action().equals(plan.action())) {
                throw new IllegalStateException("break " + breakId + " already has action "
                        + existing.get().action().get());
            }
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    UPDATE breaks SET rule_id = ?, deadline_at = ?, escalate_by = ?, review_at = ?, action = ?,
                                      resolved_at = ?, compensation_paise = ?
                     WHERE merchant_id = ? AND break_id = ?
                    """)) {
                ps.setString(1, plan.rule().name());
                ps.setObject(2, ts(plan.deadlineAt()));
                ps.setObject(3, ts(plan.escalateBy()));
                ps.setObject(4, ts(plan.reviewAt()));
                setOptionalText(ps, 5, plan.action());
                ps.setObject(6, plan.resolvedAt().map(JdbcDetectionRepos::ts).orElse(null));
                ps.setLong(7, plan.compensation().paise());
                ps.setString(8, merchantId.value());
                ps.setString(9, breakId);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save the plan of " + breakId, e);
            }
        }

        @Override
        public Optional<Plan> planOf(MerchantId merchantId, String breakId) {
            try (PreparedStatement ps = tx.connection().prepareStatement(PLAN)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, breakId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next() || rs.getString("rule_id") == null) {
                        return Optional.empty();
                    }
                    return Optional.of(new Plan(DeadlineRule.valueOf(rs.getString("rule_id")),
                            instant(rs, "deadline_at"), instant(rs, "escalate_by"), instant(rs, "review_at"),
                            Optional.ofNullable(rs.getString("action")), optionalInstant(rs, "resolved_at"),
                            Money.ofPaise(rs.getLong("compensation_paise"))));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("the plan of " + breakId, e);
            }
        }

        @Override
        public List<Break> dueForReview(Instant now, int limit) {
            try (PreparedStatement ps = tx.connection().prepareStatement(SELECT + """
                     WHERE state IN ('DETECTED', 'AWAITING_EVIDENCE', 'ACTION_PENDING')
                       AND (review_at IS NULL OR review_at <= ?)
                     ORDER BY review_at NULLS FIRST, break_id COLLATE "C"
                     LIMIT ? FOR UPDATE SKIP LOCKED
                    """)) {
                ps.setObject(1, ts(now));
                ps.setInt(2, limit);
                List<Break> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(read(rs));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("breaks due for review", e);
            }
        }

        @Override
        public Optional<Instant> nextReviewAt() {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT min(coalesce(review_at, detected_at)) AS next FROM breaks
                     WHERE state IN ('DETECTED', 'AWAITING_EVIDENCE', 'ACTION_PENDING')
                    """);
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                return optionalInstant(rs, "next");
            } catch (SQLException e) {
                throw SqlErrors.translate("next review", e);
            }
        }

        @Override
        public void wake(MerchantId merchantId, String breakId, Instant at) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    UPDATE breaks SET review_at = ? WHERE merchant_id = ? AND break_id = ? AND review_at > ?
                    """)) {
                ps.setObject(1, ts(at));
                ps.setString(2, merchantId.value());
                ps.setString(3, breakId);
                ps.setObject(4, ts(at));
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("wake " + breakId, e);
            }
        }

        @Override
        public Optional<Break> byId(String breakId) {
            List<Break> found = list(SELECT + " WHERE break_id = ?", breakId);
            return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
        }

        @Override
        public List<Break> forMerchant(MerchantId merchantId) {
            return list(SELECT + " WHERE merchant_id = ? ORDER BY type COLLATE \"C\", subject_key COLLATE \"C\"",
                    merchantId.value());
        }

        @Override
        public List<BreakTransition> transitions(MerchantId merchantId, String breakId) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT t.seq, t.from_state, t.to_state, t.at, t.reason FROM break_transitions t
                      JOIN breaks b ON b.break_id = t.break_id
                     WHERE b.merchant_id = ? AND t.break_id = ? ORDER BY t.seq
                    """)) {
                ps.setString(1, merchantId.value());
                ps.setString(2, breakId);
                List<BreakTransition> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String from = rs.getString("from_state");
                        out.add(new BreakTransition(breakId, rs.getInt("seq"),
                                from == null ? Optional.empty() : Optional.of(BreakState.valueOf(from)),
                                BreakState.valueOf(rs.getString("to_state")), instant(rs, "at"), rs.getString("reason")));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("break transitions " + breakId, e);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    static final class Settlement implements SettlementRepo {
        private static final String LINE_COLUMNS = "merchant_id, file_id, line_no, settlement_date, value_date, "
                + "amount_paise, fee_paise, tax_paise, reference, batch_id, status, candidate_hash, witnesses";
        private static final String END_COLUMNS =
                "merchant_id, file_id, settlement_date, line_count, total_paise, sha256, complete";

        private final JdbcTx tx;

        Settlement(JdbcTx tx) {
            this.tx = tx;
        }

        // ---- receipts ----

        @Override
        public Receipt receive(ReceivedFile file) {
            if (receipt(file.sha256()).isPresent()) {
                return Receipt.DUPLICATE;
            }
            Connection c = tx.connection();
            try {
                boolean conflict;
                try (PreparedStatement ps = c.prepareStatement("""
                        SELECT EXISTS (SELECT 1 FROM settlement_files WHERE merchant_id = ?
                                          AND ((status = 'ACCEPTED' AND settlement_date = ?) OR file_id = ?))
                        """)) {
                    ps.setString(1, file.merchantId().value());
                    ps.setObject(2, file.settlementDate());
                    ps.setString(3, file.fileId());
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        conflict = rs.getBoolean(1);
                    }
                }
                ReceiptStatus status = conflict ? ReceiptStatus.REJECTED_CONFLICT : ReceiptStatus.ACCEPTED;
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO settlement_files (sha256, merchant_id, file_id, settlement_date, line_count,
                                                      total_paise, received_at, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """)) {
                    ps.setString(1, file.sha256().hex());
                    ps.setString(2, file.merchantId().value());
                    ps.setString(3, file.fileId());
                    ps.setObject(4, file.settlementDate());
                    ps.setInt(5, file.lineCount());
                    ps.setLong(6, file.totalAmount().paise());
                    ps.setObject(7, ts(file.receivedAt()));
                    ps.setString(8, status.name());
                    ps.executeUpdate();
                }
                return conflict ? Receipt.CONFLICT : Receipt.NEW;
            } catch (SQLException e) {
                throw SqlErrors.translate("receive settlement file " + file.fileId(), e);
            }
        }

        @Override
        public Optional<ReceivedFile> receipt(Sha256 sha256) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    SELECT merchant_id, file_id, settlement_date, line_count, total_paise, received_at, status
                      FROM settlement_files WHERE sha256 = ?
                    """)) {
                ps.setString(1, sha256.hex());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new ReceivedFile(MerchantId.of(rs.getString("merchant_id")),
                            rs.getString("file_id"), date(rs, "settlement_date"), sha256, rs.getInt("line_count"),
                            Money.ofPaise(rs.getLong("total_paise")), instant(rs, "received_at"),
                            ReceiptStatus.valueOf(rs.getString("status"))));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("settlement receipt", e);
            }
        }

        // ---- lines ----

        private static Line line(ResultSet rs) throws SQLException {
            return new Line(MerchantId.of(rs.getString("merchant_id")), rs.getString("file_id"), rs.getInt("line_no"),
                    date(rs, "settlement_date"), date(rs, "value_date"), Money.ofPaise(rs.getLong("amount_paise")),
                    optionalMoney(rs, "fee_paise"), optionalMoney(rs, "tax_paise"),
                    Optional.ofNullable(rs.getString("reference")), Optional.ofNullable(rs.getString("batch_id")),
                    LineStatus.valueOf(rs.getString("status")),
                    Optional.ofNullable(rs.getString("candidate_hash")).map(Sha256::new),
                    decodeWitnesses(rs.getString("witnesses")));
        }

        /** Witnesses as {@code pay_1,pay_2;pay_3}: payment ids are tokens, so neither separator can occur in one. */
        static String encodeWitnesses(List<List<PaymentId>> witnesses) {
            return witnesses.stream()
                    .map(w -> String.join(",", w.stream().map(PaymentId::value).toList()))
                    .collect(Collectors.joining(";"));
        }

        static List<List<PaymentId>> decodeWitnesses(String text) {
            if (text == null) {
                return List.of();
            }
            List<List<PaymentId>> out = new ArrayList<>();
            for (String witness : text.split(";", -1)) {
                out.add(Arrays.stream(witness.split(",", -1)).map(PaymentId::of).toList());
            }
            return out;
        }

        private List<Line> lines(String where, Object... params) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT " + LINE_COLUMNS + " FROM settlement_lines WHERE " + where)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
                List<Line> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(line(rs));
                    }
                }
                return List.copyOf(out);
            } catch (SQLException e) {
                throw SqlErrors.translate("settlement lines", e);
            }
        }

        @Override
        public void saveLine(Line l) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO settlement_lines (merchant_id, file_id, line_no, settlement_date, value_date, amount_paise,
                                                  fee_paise, tax_paise, reference, batch_id, status, candidate_hash,
                                                  witnesses)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, file_id, line_no) DO UPDATE SET settlement_date = EXCLUDED.settlement_date,
                        value_date = EXCLUDED.value_date, amount_paise = EXCLUDED.amount_paise,
                        fee_paise = EXCLUDED.fee_paise, tax_paise = EXCLUDED.tax_paise, reference = EXCLUDED.reference,
                        batch_id = EXCLUDED.batch_id, status = EXCLUDED.status, candidate_hash = EXCLUDED.candidate_hash,
                        witnesses = EXCLUDED.witnesses
                    """)) {
                ps.setString(1, l.merchantId().value());
                ps.setString(2, l.fileId());
                ps.setInt(3, l.lineNo());
                ps.setObject(4, l.settlementDate());
                ps.setObject(5, l.valueDate());
                ps.setLong(6, l.amount().paise());
                setOptionalMoney(ps, 7, l.fee());
                setOptionalMoney(ps, 8, l.tax());
                setOptionalText(ps, 9, l.reference());
                setOptionalText(ps, 10, l.batchId());
                ps.setString(11, l.status().name());
                setOptionalText(ps, 12, l.candidateHash().map(Sha256::hex));
                setOptionalText(ps, 13, l.witnesses().isEmpty() ? Optional.empty()
                        : Optional.of(encodeWitnesses(l.witnesses())));
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save settlement line " + l.key(), e);
            }
        }

        @Override
        public Optional<Line> line(MerchantId merchantId, String fileId, int lineNo) {
            return lines("merchant_id = ? AND file_id = ? AND line_no = ?", merchantId.value(), fileId, lineNo)
                    .stream().findFirst();
        }

        @Override
        public List<Line> lines(MerchantId merchantId, String fileId) {
            return lines("merchant_id = ? AND file_id = ? ORDER BY line_no", merchantId.value(), fileId);
        }

        @Override
        public List<Line> linesReferencing(MerchantId merchantId, String reference) {
            return lines("merchant_id = ? AND reference = ? ORDER BY file_id COLLATE \"C\", line_no",
                    merchantId.value(), reference);
        }

        // ---- trailers ----

        private Optional<FileEnd> fileEnd(String where, Object... params) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT " + END_COLUMNS + " FROM settlement_file_ends WHERE " + where)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new FileEnd(MerchantId.of(rs.getString("merchant_id")), rs.getString("file_id"),
                            date(rs, "settlement_date"), rs.getInt("line_count"),
                            Money.ofPaise(rs.getLong("total_paise")), new Sha256(rs.getString("sha256")),
                            rs.getBoolean("complete")));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("settlement trailer", e);
            }
        }

        @Override
        public void saveFileEnd(FileEnd end) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO settlement_file_ends (merchant_id, file_id, settlement_date, line_count, total_paise,
                                                      sha256, complete)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, file_id) DO UPDATE SET settlement_date = EXCLUDED.settlement_date,
                        line_count = EXCLUDED.line_count, total_paise = EXCLUDED.total_paise, sha256 = EXCLUDED.sha256,
                        complete = EXCLUDED.complete
                    """)) {
                ps.setString(1, end.merchantId().value());
                ps.setString(2, end.fileId());
                ps.setObject(3, end.settlementDate());
                ps.setInt(4, end.lineCount());
                ps.setLong(5, end.totalAmount().paise());
                ps.setString(6, end.sha256().hex());
                ps.setBoolean(7, end.complete());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw SqlErrors.translate("save settlement trailer " + end.fileId(), e);
            }
        }

        @Override
        public Optional<FileEnd> fileEnd(MerchantId merchantId, String fileId) {
            return fileEnd("merchant_id = ? AND file_id = ?", merchantId.value(), fileId);
        }

        @Override
        public Optional<FileEnd> completeFile(MerchantId merchantId, LocalDate settlementDate) {
            return fileEnd("merchant_id = ? AND settlement_date = ? AND complete ORDER BY file_id COLLATE \"C\" LIMIT 1",
                    merchantId.value(), settlementDate);
        }

        // ---- matches ----

        @Override
        public Optional<Match> match(Match match) {
            try (PreparedStatement ps = tx.connection().prepareStatement("""
                    INSERT INTO settlement_matches (merchant_id, payment_id, file_id, line_no, how) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (merchant_id, payment_id) DO NOTHING
                    """)) {
                ps.setString(1, match.merchantId().value());
                ps.setString(2, match.paymentId().value());
                ps.setString(3, match.fileId());
                ps.setInt(4, match.lineNo());
                ps.setString(5, match.how().name());
                if (ps.executeUpdate() == 1) {
                    return Optional.empty();
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("match payment " + match.paymentId(), e);
            }
            return Optional.of(matchOf(match.merchantId(), match.paymentId()).orElseThrow(
                    () -> new IllegalStateException("match of " + match.paymentId() + " conflicted but cannot be read")));
        }

        @Override
        public Optional<Match> matchOf(MerchantId merchantId, PaymentId paymentId) {
            try (PreparedStatement ps = tx.connection().prepareStatement(
                    "SELECT file_id, line_no, how FROM settlement_matches WHERE merchant_id = ? AND payment_id = ?")) {
                ps.setString(1, merchantId.value());
                ps.setString(2, paymentId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new Match(merchantId, paymentId, rs.getString(1), rs.getInt(2),
                            MatchHow.valueOf(rs.getString(3))));
                }
            } catch (SQLException e) {
                throw SqlErrors.translate("match of " + paymentId, e);
            }
        }
    }
}
