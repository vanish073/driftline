package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.time.IstCalendar;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.sql.DataSource;

/**
 * The state of the books that the metrics of DESIGN §13 report, read from PostgreSQL in one snapshot (D78): breaks by
 * type, state and how they closed, deadlines and compensation exposure, the outbox, settlement lines, pricing drift and
 * watermarks. Read from the database rather than counted in a process, so the numbers survive restarts and are the
 * same whichever process reads them. Read-only, outside the core: not a Store method.
 */
public final class BookStats {

    /** Bounded labels for the free-text escalation reasons the resolver writes (§8); first match wins. */
    public static final List<Map.Entry<String, String>> ESCALATION_REASONS = List.of(
            Map.entry("escalate_by ", "deadline"),
            Map.entry("unreferenced line ", "batch_unresolved"),
            Map.entry("evidence gathered; ", "for_a_person"),
            Map.entry("another break owns ", "action_owner_conflict"),
            Map.entry("the gateway reported the refund failed", "refund_failed"),
            Map.entry("the payment is no longer a clean success", "not_clean_success"),
            Map.entry("the order was paid with this payment ", "paid_after_refund_decided"),
            Map.entry("the order has an open IDEMPOTENCY_CONFLICT", "idempotency_conflict"),
            Map.entry("the duplicate names no kept payment", "duplicate_unconfirmed"),
            Map.entry("the kept payment is not confirmed ", "duplicate_unconfirmed"),
            Map.entry("the gateway does not confirm the success ", "success_unconfirmed"),
            Map.entry("the merchant has never reported this order", "order_unknown"));

    /** Open breaks: not final, not following another (LINKED). An escalated break is open until a person closes it. */
    public static final List<String> OPEN_STATES = List.of("DETECTED", "AWAITING_EVIDENCE", "ACTION_PENDING",
            "ESCALATED");

    /** One stream watermark; {@code source} is GATEWAY, MERCHANT or BANK (the settlement watermark). */
    public record Watermark(int partition, String source, Instant value) {
    }

    /**
     * @param detected            every break ever raised, by type
     * @param closed              closing transitions by type, then how ({@code auto}, {@code self_healed}, {@code manual})
     * @param escalated           escalations by type, then bounded reason ({@link #ESCALATION_REASONS}, else {@code other})
     * @param open                open breaks by type, then state
     * @param earliestDeadline    the earliest deadline among the open, planned breaks of each type
     * @param compensationRupees  what the open breaks would owe under the RBI rules if resolved now
     * @param outboxPending       commands not DONE, by kind
     * @param oldestPendingSince  when the oldest command not DONE was first sent, or became due if never sent
     * @param lines               settlement lines: {@code received} (all), then the count in each status
     * @param pricingDrift        short lines explained by itemised fee and tax that differ from the schedule
     */
    public record Snapshot(SortedMap<String, Long> detected, SortedMap<String, SortedMap<String, Long>> closed,
                           SortedMap<String, SortedMap<String, Long>> escalated,
                           SortedMap<String, SortedMap<String, Long>> open, SortedMap<String, Instant> earliestDeadline,
                           long compensationRupees, SortedMap<String, Long> outboxPending,
                           Optional<Instant> oldestPendingSince, SortedMap<String, Long> lines, long pricingDrift,
                           List<Watermark> watermarks) {
    }

    private final DataSource dataSource;

    public BookStats(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @FunctionalInterface
    private interface Rows {
        void row(ResultSet rs) throws SQLException;
    }

    private static void query(Connection c, String sql, Rows rows) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                rows.row(rs);
            }
        }
    }

    private static String reasonCase() {
        StringBuilder sql = new StringBuilder("CASE");
        for (Map.Entry<String, String> e : ESCALATION_REASONS) {
            sql.append(" WHEN starts_with(t.reason, '").append(e.getKey().replace("'", "''")).append("') THEN '")
                    .append(e.getValue()).append('\'');
        }
        return sql.append(" ELSE 'other' END").toString();
    }

    private static final String OPEN_IN = "('DETECTED', 'AWAITING_EVIDENCE', 'ACTION_PENDING', 'ESCALATED')";

    /** Reads everything in one REPEATABLE READ, read-only transaction, so the numbers agree with one another. */
    public Snapshot snapshot(Instant now) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            c.setReadOnly(true);
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try {
                Snapshot s = read(c, now);
                c.commit();
                return s;
            } catch (SQLException | RuntimeException failure) {
                try {
                    c.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException e) {
            throw SqlErrors.translate("read book stats", e);
        }
    }

    private static Snapshot read(Connection c, Instant now) throws SQLException {
        TreeMap<String, Long> detected = new TreeMap<>();
        query(c, "SELECT type, count(*) FROM breaks GROUP BY type", rs -> detected.put(rs.getString(1), rs.getLong(2)));

        TreeMap<String, SortedMap<String, Long>> closed = new TreeMap<>();
        query(c, """
                SELECT b.type, CASE t.to_state WHEN 'RESOLVED_AUTO' THEN 'auto' WHEN 'SELF_HEALED' THEN 'self_healed'
                                               ELSE 'manual' END, count(*)
                  FROM break_transitions t JOIN breaks b ON b.break_id = t.break_id
                 WHERE t.to_state IN ('RESOLVED_AUTO', 'SELF_HEALED', 'RESOLVED_MANUAL')
                 GROUP BY 1, 2
                """, rs -> nested(closed, rs));

        TreeMap<String, SortedMap<String, Long>> escalated = new TreeMap<>();
        query(c, "SELECT b.type, " + reasonCase() + ", count(*) FROM break_transitions t JOIN breaks b"
                + " ON b.break_id = t.break_id WHERE t.to_state = 'ESCALATED' GROUP BY 1, 2", rs -> nested(escalated, rs));

        TreeMap<String, SortedMap<String, Long>> open = new TreeMap<>();
        query(c, "SELECT type, state, count(*) FROM breaks WHERE state IN " + OPEN_IN + " GROUP BY 1, 2",
                rs -> nested(open, rs));

        TreeMap<String, Instant> earliest = new TreeMap<>();
        long[] compensation = {0};
        query(c, "SELECT type, rule_id, deadline_at FROM breaks WHERE state IN " + OPEN_IN
                + " AND deadline_at IS NOT NULL", rs -> {
                    String type = rs.getString(1);
                    Instant deadline = rs.getTimestamp(3).toInstant();
                    earliest.merge(type, deadline, (a, b) -> a.isBefore(b) ? a : b);
                    Money owed = DeadlineRule.valueOf(rs.getString(2)).compensation(deadline, now);
                    compensation[0] = Math.addExact(compensation[0], owed.paise() / 100);
                });

        TreeMap<String, Long> pending = new TreeMap<>();
        query(c, "SELECT kind, count(*) FROM outbox WHERE status <> 'DONE' GROUP BY kind",
                rs -> pending.put(rs.getString(1), rs.getLong(2)));
        List<Instant> oldest = new ArrayList<>(1);
        query(c, "SELECT min(coalesce(first_sent_at, next_attempt_at)) FROM outbox WHERE status <> 'DONE'", rs -> {
            Timestamp t = rs.getTimestamp(1);
            if (t != null) {
                oldest.add(t.toInstant());
            }
        });

        TreeMap<String, Long> lines = new TreeMap<>();
        lines.put("received", 0L);
        query(c, "SELECT status, count(*) FROM settlement_lines GROUP BY status", rs -> {
            lines.put(rs.getString(1), rs.getLong(2));
            lines.merge("received", rs.getLong(2), Math::addExact);
        });

        long[] drift = {0};
        query(c, """
                SELECT count(*) FROM breaks
                 WHERE type = 'SHORT_SETTLEMENT' AND evidence->>'pricing_drift' <> '0.00'
                """, rs -> drift[0] = rs.getLong(1));

        List<Watermark> watermarks = new ArrayList<>();
        query(c, "SELECT partition, source, value FROM watermarks ORDER BY partition, source",
                rs -> watermarks.add(new Watermark(rs.getInt(1), rs.getString(2), rs.getTimestamp(3).toInstant())));
        query(c, """
                SELECT m.partition, min(w.complete_through) FROM settlement_watermarks w
                  JOIN merchants m ON m.merchant_id = w.merchant_id GROUP BY m.partition ORDER BY m.partition
                """, rs -> {
                    LocalDate through = rs.getObject(2, LocalDate.class);
                    watermarks.add(new Watermark(rs.getInt(1), "BANK", IstCalendar.startOfIstDay(through.plusDays(1))));
                });

        return new Snapshot(unmodifiable(detected), unmodifiableNested(closed), unmodifiableNested(escalated),
                unmodifiableNested(open), Collections.unmodifiableSortedMap(earliest), compensation[0],
                unmodifiable(pending), oldest.stream().findFirst(), unmodifiable(lines), drift[0], List.copyOf(watermarks));
    }

    private static void nested(TreeMap<String, SortedMap<String, Long>> into, ResultSet rs) throws SQLException {
        into.computeIfAbsent(rs.getString(1), k -> new TreeMap<>()).put(rs.getString(2), rs.getLong(3));
    }

    private static SortedMap<String, Long> unmodifiable(TreeMap<String, Long> m) {
        return Collections.unmodifiableSortedMap(m);
    }

    private static SortedMap<String, SortedMap<String, Long>> unmodifiableNested(
            TreeMap<String, SortedMap<String, Long>> m) {
        TreeMap<String, SortedMap<String, Long>> out = new TreeMap<>();
        m.forEach((k, v) -> out.put(k, Collections.unmodifiableSortedMap(new TreeMap<>(v))));
        return Collections.unmodifiableSortedMap(out);
    }
}
