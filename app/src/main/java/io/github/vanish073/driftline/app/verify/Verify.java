package io.github.vanish073.driftline.app.verify;

import io.github.vanish073.driftline.core.audit.AuditChain;
import io.github.vanish073.driftline.core.breaks.BreakLifecycle;
import io.github.vanish073.driftline.store.postgres.BalanceStrategy;
import io.github.vanish073.driftline.store.postgres.PostgresStore;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * {@code driftline verify} (DRIFTLINE_ROLE=verify; Shift 8, {@code scripts/chaos.sh}): after a run, checks the books
 * and the world against each other with read-only SQL and the sandbox's refund count. Each check counts violations;
 * zero passes. The checks are re-run until they all pass or {@code wait} runs out, because a refund the gateway made a
 * moment ago may still be on its way to the books; the last result is the verdict.
 *
 * <ul>
 *   <li><b>ledger balanced</b>: every transaction's entries sum to zero (INV-LEDGER-BALANCED).</li>
 *   <li><b>one refund command per payment</b>: refund keys are payment-scoped and one break acts for a payment (D11).</li>
 *   <li><b>transitions legal</b>: every recorded break transition is in the lifecycle table (§8.1, D60).</li>
 *   <li><b>every break reviewed</b>: no break left DETECTED and unplanned 10 minutes after it was detected.</li>
 *   <li><b>deadlines met</b>: no break still unhandled an hour after its escalate_by (INV-DEADLINE, D63).</li>
 *   <li><b>audit chain intact</b>: {@code verify-audit}'s walk of the hash chain (§7.6, {@link AuditChain}).</li>
 *   <li><b>outbox drained</b>: no command left unclaimed 2 minutes after it was due (the dispatcher polls every
 *       second; a retry waiting out its backoff is not yet due).</li>
 *   <li><b>refunds once, and booked</b> (with the sandbox): no payment refunded more than once by the gateway
 *       (INV-REFUND-ONCE), and the REFUND ledger transactions equal the refunds the gateway executed
 *       (INV-BOOKS-MATCH-WORLD, for refunds).</li>
 * </ul>
 */
public final class Verify {

    /** One check's outcome: how many violations it found. */
    public record Check(String name, long violations) {
        public boolean passed() {
            return violations == 0;
        }
    }

    private final DataSource dataSource;
    private final Optional<URI> sandbox;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public Verify(DataSource dataSource, Optional<URI> sandbox) {
        this.dataSource = dataSource;
        this.sandbox = sandbox;
    }

    /** Runs the checks until they pass or {@code wait} runs out; prints each round's failures and the final table. */
    public boolean run(Duration wait, Duration every, PrintStream out) throws InterruptedException {
        Instant deadline = Instant.now().plus(wait);
        List<Check> checks = checks();
        while (!checks.stream().allMatch(Check::passed) && Instant.now().isBefore(deadline)) {
            out.println("verify: not yet: " + checks.stream().filter(c -> !c.passed())
                    .map(c -> c.name() + "=" + c.violations()).collect(Collectors.joining(", ")));
            Thread.sleep(every.toMillis());
            checks = checks();
        }
        for (Check c : checks) {
            out.printf("verify: %s %-40s %d%n", c.passed() ? "PASS" : "FAIL", c.name(), c.violations());
        }
        boolean ok = checks.stream().allMatch(Check::passed);
        out.println("verify: " + (ok ? "CLEAN" : "VIOLATIONS FOUND"));
        return ok;
    }

    /** One round of every check. */
    public List<Check> checks() {
        Map<String, String> sql = new LinkedHashMap<>();
        sql.put("ledger balanced", """
                SELECT count(*) FROM (SELECT txn_id FROM ledger_entries GROUP BY txn_id
                                       HAVING sum(amount_paise) <> 0) unbalanced""");
        sql.put("one refund command per payment", """
                SELECT count(*) FROM (SELECT merchant_id, args->>'payment_id' FROM outbox WHERE kind = 'REFUND'
                                       GROUP BY 1, 2 HAVING count(*) > 1) twice""");
        sql.put("transitions legal", "SELECT count(*) FROM break_transitions WHERE from_state IS NOT NULL"
                + " AND (from_state, to_state) NOT IN (" + legalMoves() + ")");
        sql.put("every break reviewed", """
                SELECT count(*) FROM breaks WHERE state = 'DETECTED' AND review_at IS NULL
                                              AND detected_at < now() - interval '10 minutes'""");
        sql.put("deadlines met", """
                SELECT count(*) FROM breaks WHERE state IN ('DETECTED', 'AWAITING_EVIDENCE', 'ACTION_PENDING')
                                              AND escalate_by < now() - interval '1 hour'""");
        sql.put("outbox drained", """
                SELECT count(*) FROM outbox WHERE status <> 'DONE'
                                              AND next_attempt_at < now() - interval '2 minutes'""");
        List<Check> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection()) {
            c.setReadOnly(true);
            for (Map.Entry<String, String> e : sql.entrySet()) {
                out.add(new Check(e.getKey(), count(c, e.getValue())));
            }
            out.add(new Check("audit chain intact", auditChain(dataSource).intact() ? 0 : 1));
            if (sandbox.isPresent()) {
                JsonObject stats = stats(sandbox.get());
                out.add(new Check("no payment refunded twice (gateway)",
                        stats.integer("payments_refunded_more_than_once")));
                long booked = count(c, "SELECT count(*) FROM ledger_transactions WHERE cause_type = 'REFUND'");
                out.add(new Check("refunds booked = refunds executed", Math.abs(booked - stats.integer("refunds"))));
            }
            c.rollback();
        } catch (SQLException e) {
            throw new IllegalStateException("verify cannot read the database: " + e.getMessage(), e);
        }
        return out;
    }

    /** Walks the audit chain through the store's own reader, read-only. The data source stays open. */
    public static AuditChain.Verdict auditChain(DataSource dataSource) {
        return AuditChain.verify(new PostgresStore(dataSource, BalanceStrategy.APPEND_SNAPSHOT)
                .transact("verify-audit", tx -> tx.audit().rows()));
    }

    private static String legalMoves() {
        return BreakLifecycle.MOVES.stream().map(m -> "('" + m.from().name() + "', '" + m.to().name() + "')")
                .collect(Collectors.joining(", "));
    }

    private static long count(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private JsonObject stats(URI base) {
        try {
            HttpResponse<byte[]> r = http.send(HttpRequest.newBuilder(base.resolve("/v1/stats"))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            if (r.statusCode() != 200) {
                throw new IllegalStateException("the sandbox answered " + r.statusCode());
            }
            return StrictJson.parse(r.body());
        } catch (IOException e) {
            throw new IllegalStateException("verify cannot reach the sandbox: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
