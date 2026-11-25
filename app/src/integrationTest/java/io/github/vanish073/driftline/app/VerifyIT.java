package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.app.verify.Verify;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.store.postgres.BalanceStrategy;
import io.github.vanish073.driftline.store.postgres.PostgresStore;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** {@code driftline verify} (DRIFTLINE_ROLE=verify): clean books pass; a planted violation fails the process. */
class VerifyIT {

    @Test
    void cleanBooksPassAndAStuckOutboxCommandFailsTheRole() throws Exception {
        String url = Containers.newDatabase("verify_it");
        try (HikariDataSource pool = Containers.appPool(url, 2)) {
            Verify verify = new Verify(pool, Optional.empty());
            assertThat(verify.checks()).allMatch(Verify.Check::passed).hasSize(7);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            assertThat(verify.run(Duration.ZERO, Duration.ofMillis(10), new PrintStream(out, true,
                    StandardCharsets.UTF_8))).isTrue();
            assertThat(out.toString(StandardCharsets.UTF_8)).contains("verify: CLEAN");
            assertThat(role(url)).as("the role exits cleanly").isNull();

            // a break-less probe command due an hour ago and never done: the dispatcher is not draining the outbox
            try (Connection c = Containers.superuser(url); Statement st = c.createStatement()) {
                st.execute("""
                        INSERT INTO outbox (command_id, merchant_id, break_id, kind, args, status, attempt,
                                            next_attempt_at)
                        VALUES ('cmd_stuck', 'm_1', NULL, 'STATUS_QUERY_ORDER', '{"order_id":"ord_1"}', 'PENDING', 0,
                                now() - interval '1 hour')""");
            }
            assertThat(verify.checks()).filteredOn(c -> !c.passed()).extracting(Verify.Check::name)
                    .containsExactly("outbox drained");
            assertThat(role(url)).as("the role fails").isNotNull()
                    .hasStackTraceContaining("driftline verify found violations");
        }
    }

    /** Runs the {@code verify} role once; returns what it threw, or null if it exited cleanly. */
    private static Throwable role(String url) {
        return role("verify", url);
    }

    /** Runs a one-shot role once; returns what it threw, or null if it exited cleanly. */
    private static Throwable role(String role, String url) {
        try {
            DriftlineApplication.application(role).run(
                    "--spring.datasource.url=" + url,
                    "--spring.datasource.username=" + Containers.APP_USER,
                    "--spring.datasource.password=" + Containers.APP_PASSWORD,
                    "--driftline.verify.wait-for=0s", "--driftline.environment=test").close();
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    @Test
    void verifyAuditPassesAnIntactChainAndFailsOnceARowWasChangedAroundTheGuards() throws Exception {
        String url = Containers.newDatabase("verify_audit_it");
        try (HikariDataSource pool = Containers.appPool(url, 2)) {
            PostgresStore store = new PostgresStore(pool, BalanceStrategy.APPEND_SNAPSHOT);
            for (int i = 1; i <= 2; i++) {
                AuditRepo.Entry e = AuditRepo.Entry.of(Instant.parse("2026-11-17T06:00:00Z").plusSeconds(i),
                        "ops/abc", "BREAK_RESOLVE_MANUAL", "brk_" + i, Map.of("note", "row " + i));
                store.transact("append", tx -> tx.audit().append(e));
            }
            assertThat(role("verify-audit", url)).as("an intact chain").isNull();
            try (Connection c = Containers.superuser(url); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("SET LOCAL session_replication_role = replica");   // a superuser switching triggers off
                st.execute("UPDATE audit_log SET actor = 'ops/someone-else' WHERE seq = 1");
                c.commit();
            }
            assertThat(role("verify-audit", url)).isNotNull()
                    .hasStackTraceContaining("the audit chain is broken at row 1");
            assertThat(new Verify(pool, Optional.empty()).checks()).filteredOn(ch -> !ch.passed())
                    .extracting(Verify.Check::name).containsExactly("audit chain intact");
        }
    }
}
