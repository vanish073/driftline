package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The tables behind §13's metrics and traces (D78), as the application role: the book snapshot counts breaks, closings,
 * bounded escalation reasons, deadlines, compensation exposure, the outbox, settlement lines, pricing drift and
 * watermarks exactly; trace contexts are written once and never changed.
 */
class ObservabilityTablesIT {

    static final Instant NOW = Instant.parse("2026-11-20T06:00:00Z");
    static final String TP1 = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    static final String TP2 = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01";

    static HikariDataSource pool(String url) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(PostgresFixture.APP_USER);
        config.setPassword(PostgresFixture.APP_PASSWORD);
        config.setMaximumPoolSize(2);
        config.setAutoCommit(false);
        return new HikariDataSource(config);
    }

    /** A break row as the matcher and resolver leave it; {@code deadline} null means not planned yet. */
    static String brk(String id, String type, String state, String rule, String deadline, String evidence) {
        String plan = rule == null ? "NULL, NULL, NULL, NULL, NULL"
                : "'" + rule + "', '" + deadline + "', '" + deadline + "', '" + deadline + "', 0";
        return "INSERT INTO breaks (break_id, merchant_id, kind, type, subject_key, state, detected_at, evidence, version,"
                + " rule_id, deadline_at, escalate_by, review_at, compensation_paise) VALUES ('" + id + "', 'm_o', '"
                + (type.equals("STATUS_CONFLICT") ? "ANOMALY" : "BREAK") + "', '" + type + "', '" + id + "', '" + state
                + "', '2026-11-15T00:00:00Z', '" + evidence + "', 1, " + plan + ")";
    }

    static String transition(String id, int seq, String to, String reason) {
        return "INSERT INTO break_transitions (break_id, seq, from_state, to_state, at, reason) VALUES ('" + id + "', "
                + seq + ", " + (seq == 1 ? "NULL" : "'DETECTED'") + ", '" + to + "', '2026-11-16T00:00:00Z', '"
                + reason + "')";
    }

    @Test
    void theBookSnapshotCountsWhatTheMetricsReport() throws SQLException {
        String url = PostgresFixture.newDatabase("observability_book_it");
        try (Connection c = PostgresFixture.superuser(url); Statement st = c.createStatement()) {
            st.execute("INSERT INTO merchants VALUES ('m_o', 3, 200, 1800, 1, 'DAILY', '2026-11-10', '2026-11-01T00:00Z')");
            // an RBI break 3 IST days past its deadline (₹300), an internal one due in 2 hours, an escalated one,
            // a closed duplicate, a self-healed orphan, and a short line whose fee drifted from the schedule
            st.execute(brk("brk_late", "DEBITED_NO_ORDER", "ACTION_PENDING", "RBI_UPI_P2M_T5", "2026-11-17T18:30:00Z",
                    "{}"));
            st.execute(brk("brk_short", "SHORT_SETTLEMENT", "DETECTED", "INTERNAL_ESCALATE_4H", "2026-11-20T08:00:00Z",
                    "{\"pricing_drift\": \"1.20\"}"));
            st.execute(brk("brk_exact", "SHORT_SETTLEMENT", "RESOLVED_AUTO", null, null, "{\"pricing_drift\": \"0.00\"}"));
            st.execute(brk("brk_conf", "STATUS_CONFLICT", "ESCALATED", "INTERNAL_ESCALATE_4H", "2026-11-19T00:00:00Z",
                    "{}"));
            st.execute(brk("brk_dup", "DUPLICATE_CAPTURE", "RESOLVED_MANUAL", "INTERNAL_DUP_T1", "2026-11-18T18:30:00Z",
                    "{}"));
            st.execute(brk("brk_orph", "ORPHAN_SETTLEMENT", "SELF_HEALED", "INTERNAL_ESCALATE_4H",
                    "2026-11-16T04:00:00Z", "{}"));
            st.execute(brk("brk_new", "DEBITED_NO_ORDER", "DETECTED", null, null, "{}"));
            for (String id : List.of("brk_late", "brk_short", "brk_exact", "brk_conf", "brk_dup", "brk_orph", "brk_new")) {
                st.execute(transition(id, 1, "DETECTED", "raised"));
            }
            st.execute(transition("brk_exact", 2, "RESOLVED_AUTO", "explained by itemised fee and tax"));
            st.execute(transition("brk_conf", 2, "ESCALATED", "evidence gathered; STATUS_CONFLICT is for a person (§8.2)"));
            st.execute(transition("brk_dup", 2, "ESCALATED", "escalate_by 2026-11-18T18:30:00Z has passed"));
            st.execute("INSERT INTO break_transitions VALUES ('brk_dup', 3, 'ESCALATED', 'RESOLVED_MANUAL',"
                    + " '2026-11-17T00:00:00Z', 'refunded by hand')");
            st.execute(transition("brk_orph", 2, "SELF_HEALED", "attributed to pay_9"));
            st.execute("INSERT INTO outbox (command_id, merchant_id, break_id, kind, args, status, attempt,"
                    + " next_attempt_at, first_sent_at) VALUES"
                    + " ('cmd_a', 'm_o', 'brk_late', 'REFUND', '{}', 'PENDING', 1, '2026-11-20T06:05:00Z',"
                    + "  '2026-11-20T05:00:00Z'),"
                    + " ('cmd_b', 'm_o', 'brk_late', 'STATUS_QUERY_PAYMENT', '{}', 'PENDING', 0, '2026-11-20T05:30:00Z',"
                    + "  NULL)");
            st.execute("INSERT INTO outbox (command_id, merchant_id, break_id, kind, args, status, attempt,"
                    + " next_attempt_at, first_sent_at, result) VALUES ('cmd_c', 'm_o', 'brk_dup', 'REFUND', '{}', 'DONE',"
                    + " 1, '2026-11-17T00:00:00Z', '2026-11-17T00:00:00Z', '{}')");
            st.execute("INSERT INTO settlement_lines (merchant_id, file_id, line_no, settlement_date, value_date,"
                    + " amount_paise, status) VALUES ('m_o', 'f1', 1, '2026-11-18', '2026-11-18', 100, 'MATCHED'),"
                    + " ('m_o', 'f1', 2, '2026-11-18', '2026-11-18', 100, 'MATCHED'),"
                    + " ('m_o', 'f1', 3, '2026-11-18', '2026-11-18', 100, 'AWAITING_BATCH')");
            st.execute("INSERT INTO watermarks VALUES (3, 'GATEWAY', '2026-11-20T05:59:00Z', NULL, NULL)");
            st.execute("INSERT INTO settlement_watermarks VALUES ('m_o', '2026-11-18')");
        }
        try (HikariDataSource pool = pool(url)) {
            BookStats.Snapshot s = new BookStats(pool).snapshot(NOW);
            assertThat(s.detected()).containsExactlyEntriesOf(new TreeMap<>(Map.of("DEBITED_NO_ORDER", 2L,
                    "DUPLICATE_CAPTURE", 1L, "ORPHAN_SETTLEMENT", 1L, "SHORT_SETTLEMENT", 2L, "STATUS_CONFLICT", 1L)));
            assertThat(s.closed()).isEqualTo(new TreeMap<>(Map.of("SHORT_SETTLEMENT", new TreeMap<>(Map.of("auto", 1L)),
                    "DUPLICATE_CAPTURE", new TreeMap<>(Map.of("manual", 1L)),
                    "ORPHAN_SETTLEMENT", new TreeMap<>(Map.of("self_healed", 1L)))));
            assertThat(s.escalated()).as("free-text reasons become bounded labels").isEqualTo(new TreeMap<>(Map.of(
                    "STATUS_CONFLICT", new TreeMap<>(Map.of("for_a_person", 1L)),
                    "DUPLICATE_CAPTURE", new TreeMap<>(Map.of("deadline", 1L)))));
            assertThat(s.open()).isEqualTo(new TreeMap<>(Map.of(
                    "DEBITED_NO_ORDER", new TreeMap<>(Map.of("ACTION_PENDING", 1L, "DETECTED", 1L)),
                    "SHORT_SETTLEMENT", new TreeMap<>(Map.of("DETECTED", 1L)),
                    "STATUS_CONFLICT", new TreeMap<>(Map.of("ESCALATED", 1L)))));
            assertThat(s.earliestDeadline()).containsExactlyEntriesOf(new TreeMap<>(Map.of(
                    "DEBITED_NO_ORDER", Instant.parse("2026-11-17T18:30:00Z"),
                    "SHORT_SETTLEMENT", Instant.parse("2026-11-20T08:00:00Z"),
                    "STATUS_CONFLICT", Instant.parse("2026-11-19T00:00:00Z"))));
            assertThat(s.compensationRupees())
                    .as("₹100 for each IST day after the deadline date (the 17th): the 18th, 19th and 20th").isEqualTo(300);
            assertThat(s.outboxPending()).containsExactlyEntriesOf(new TreeMap<>(Map.of("REFUND", 1L,
                    "STATUS_QUERY_PAYMENT", 1L)));
            assertThat(s.oldestPendingSince()).contains(Instant.parse("2026-11-20T05:00:00Z"));
            assertThat(s.lines()).containsExactlyEntriesOf(new TreeMap<>(Map.of("received", 3L, "MATCHED", 2L,
                    "AWAITING_BATCH", 1L)));
            assertThat(s.pricingDrift()).isEqualTo(1);
            assertThat(s.watermarks()).containsExactly(
                    new BookStats.Watermark(3, "GATEWAY", Instant.parse("2026-11-20T05:59:00Z")),
                    new BookStats.Watermark(3, "BANK", Instant.parse("2026-11-18T18:30:00Z")));
        }
    }

    @Test
    void aTraceContextIsWrittenOnceAndNeverChanged() throws SQLException {
        String url = PostgresFixture.newDatabase("observability_trace_it");
        try (HikariDataSource pool = pool(url)) {
            TraceContexts traces = new TraceContexts(pool);
            traces.save(new TreeMap<>(Map.of("brk_a", TP1, "cmd_b", TP1)), NOW);
            traces.save(new TreeMap<>(Map.of("brk_a", TP2)), NOW.plusSeconds(1));
            assertThat(traces.find(List.of("brk_a", "cmd_b", "cmd_none")))
                    .as("the first context wins").containsExactlyEntriesOf(new TreeMap<>(Map.of("brk_a", TP1, "cmd_b", TP1)));
            assertThatThrownBy(() -> traces.save(new TreeMap<>(Map.of("pay_1", TP1)), NOW))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> traces.save(new TreeMap<>(Map.of("brk_c", "not-a-traceparent")), NOW))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        try (Connection c = PostgresFixture.superuser(url); Statement st = c.createStatement()) {
            assertThatThrownBy(() -> st.execute("UPDATE trace_contexts SET traceparent = '" + TP2 + "'"))
                    .as("append-only, even for the owner").isInstanceOf(SQLException.class);
        }
    }
}
