package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.app.verify.Verify;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** {@code driftline verify} (DRIFTLINE_ROLE=verify): clean books pass; a planted violation fails the process. */
class VerifyIT {

    @Test
    void cleanBooksPassAndAStuckOutboxCommandFailsTheRole() throws Exception {
        String url = Containers.newDatabase("verify_it");
        try (HikariDataSource pool = Containers.appPool(url, 2)) {
            Verify verify = new Verify(pool, Optional.empty());
            assertThat(verify.checks()).allMatch(Verify.Check::passed).hasSize(6);
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

    /** Runs the role once; returns what it threw, or null if it exited cleanly. */
    private static Throwable role(String url) {
        try {
            DriftlineApplication.application("verify").run(
                    "--spring.datasource.url=" + url,
                    "--spring.datasource.username=" + Containers.APP_USER,
                    "--spring.datasource.password=" + Containers.APP_PASSWORD,
                    "--driftline.verify.wait-for=0s").close();
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }
}
