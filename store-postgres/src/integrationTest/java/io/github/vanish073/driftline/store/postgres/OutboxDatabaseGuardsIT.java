package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.OutboxRepo.Command;
import io.github.vanish073.driftline.core.port.OutboxRepo.CommandKind;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * What PostgreSQL itself enforces for the outbox and break plans (V7), as the application role: a DONE command is a
 * fact that never changes, no command is ever deleted, a break's action is set at most once, and two dispatchers that
 * claim at the same moment never get the same command (§9.2: {@code FOR UPDATE SKIP LOCKED}).
 */
class OutboxDatabaseGuardsIT {

    static final Instant T0 = Instant.parse("2026-11-14T06:00:00Z");
    static final MerchantId M = MerchantId.of("m_guard");

    private static PostgresStore storeWithCommands(String url, int commands) {
        PostgresStore store = PostgresFixture.store(url, BalanceStrategy.APPEND_SNAPSHOT, 4);
        store.transact("setup", tx -> {
            Break b = Break.detected(M, BreakType.DEBITED_NO_ORDER, "pay_1", T0, Map.of());
            tx.breaks().raise(b, "setup");
            for (int i = 0; i < commands; i++) {
                tx.outbox().enqueue(Command.pending(String.format("cmd_%03d", i), M, Optional.of(b.breakId()),
                        CommandKind.STATUS_QUERY_PAYMENT, Map.of("payment_id", "pay_1"), Optional.empty(), T0));
            }
            return null;
        });
        return store;
    }

    private static void sql(Connection c, String statement) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.executeUpdate(statement);
        }
        c.commit();
    }

    @Test
    void aDoneCommandNeverChangesAndNoCommandIsEverDeleted() throws SQLException {
        String url = PostgresFixture.newDatabase("outbox_guards");
        PostgresStore store = storeWithCommands(url, 2);
        store.transact("claim", tx -> tx.outbox().claim(T0, "d1", T0.plusSeconds(30), T0.plusSeconds(40), 1));
        store.transact("done", tx -> tx.outbox().complete("cmd_000", Map.of("status", "SUCCEEDED")));
        try (Connection c = PostgresFixture.app(url)) {
            assertThatThrownBy(() -> sql(c, "UPDATE outbox SET attempt = 9 WHERE command_id = 'cmd_000'"))
                    .hasMessageContaining("is DONE and cannot change");
            c.rollback();
            assertThatThrownBy(() -> sql(c, "DELETE FROM outbox WHERE command_id = 'cmd_001'"))
                    .isInstanceOf(SQLException.class);
            c.rollback();
            assertThatThrownBy(() -> sql(c, "UPDATE outbox SET status = 'DONE' WHERE command_id = 'cmd_001'"))
                    .as("DONE needs a result").isInstanceOf(SQLException.class);
            c.rollback();
        }
    }

    @Test
    void aBreaksActionIsSetAtMostOnceEvenByRawSql() throws SQLException {
        String url = PostgresFixture.newDatabase("action_guards");
        storeWithCommands(url, 0);
        try (Connection c = PostgresFixture.app(url)) {
            sql(c, "UPDATE breaks SET rule_id = 'RBI_UPI_P2M_T5', deadline_at = now(), escalate_by = now(),"
                    + " review_at = now(), compensation_paise = 0, action = 'REFUND'");
            assertThatThrownBy(() -> sql(c, "UPDATE breaks SET action = 'ESCALATE'"))
                    .hasMessageContaining("cannot change");
            c.rollback();
            sql(c, "UPDATE breaks SET review_at = now() + interval '1 hour'");   // other plan fields may change
            assertThatThrownBy(() -> sql(c, "UPDATE breaks SET deadline_at = NULL"))
                    .as("a plan is all or none").isInstanceOf(SQLException.class);
            c.rollback();
        }
    }

    @Test
    void twoDispatchersClaimingAtOnceNeverGetTheSameCommand() throws Exception {
        String url = PostgresFixture.newDatabase("outbox_claims");
        PostgresStore store = storeWithCommands(url, 40);
        CyclicBarrier together = new CyclicBarrier(2);
        List<List<String>> claimed = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        CountDownLatch done = new CountDownLatch(2);
        for (String owner : List.of("d1", "d2")) {
            Thread.ofPlatform().start(() -> {
                try {
                    together.await(10, TimeUnit.SECONDS);
                    List<String> ids = store.transact("claim-" + owner, tx -> {
                        List<String> out = new ArrayList<>();
                        tx.outbox().claim(T0, owner, T0.plusSeconds(30), T0.plusSeconds(40), 25)
                                .forEach(c -> out.add(c.commandId()));
                        return out;
                    });
                    synchronized (claimed) {
                        claimed.add(ids);
                    }
                } catch (Exception e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(errors).isEmpty();
        TreeSet<String> all = new TreeSet<>();
        int total = 0;
        for (List<String> ids : claimed) {
            all.addAll(ids);
            total += ids.size();
        }
        assertThat(total).as("no command was claimed twice").isEqualTo(all.size());
        assertThat(all).as("between them, both claims took every command they could").hasSize(40);
    }
}
