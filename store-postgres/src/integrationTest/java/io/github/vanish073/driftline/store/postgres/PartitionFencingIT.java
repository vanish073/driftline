package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.DedupeRepo;
import io.github.vanish073.driftline.core.port.StoreConflict;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Fencing under real concurrency (DESIGN §5.1, §11.9), which the one-at-a-time simulator store cannot show: a zombie
 * whose commit runs while a new owner's acquire is still uncommitted waits for the row lock, re-checks the epoch
 * under READ COMMITTED, and is fenced. Its writes roll back with it. The database also refuses to move a lease back.
 */
class PartitionFencingIT {

    private static final String URL = PostgresFixture.newDatabase("partition_fencing");
    private static final PostgresStore STORE = PostgresFixture.store(URL, BalanceStrategy.APPEND_SNAPSHOT, 4);
    private static final MerchantId M = MerchantId.of("m_fence");
    private static final Sha256 HASH = Sha256.of("zombie".getBytes(StandardCharsets.UTF_8));

    @Test
    void aZombieCommitThatWaitsOnANewOwnerIsFenced() throws Exception {
        int p = 7;
        long zombieEpoch = STORE.transact("assign-a", tx -> tx.partitions().acquire(p, "matcher-a.1")).epoch();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch acquired = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Future<Long> newOwner = pool.submit(() -> STORE.transact("assign-b", tx -> {
                long epoch = tx.partitions().acquire(p, "matcher-b.1").epoch();
                acquired.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);   // hold the row lock, uncommitted
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return epoch;
            }));
            assertThat(acquired.await(30, TimeUnit.SECONDS)).isTrue();
            Future<Void> zombie = pool.submit(() -> STORE.transact("zombie-batch", tx -> {
                tx.dedupe().claim(M, Source.GATEWAY, "evt_z", HASH, p, 0);
                tx.partitions().commitOffset(p, zombieEpoch, 1);
                return null;
            }));
            awaitOneBackendWaitingOnALock();
            release.countDown();
            assertThat(newOwner.get(30, TimeUnit.SECONDS)).isEqualTo(zombieEpoch + 1);
            assertThatThrownBy(() -> zombie.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).cause().isInstanceOf(StoreConflict.class);
        } finally {
            pool.shutdownNow();
        }
        long offset = STORE.transact("read", tx -> tx.partitions().lease(p).nextOffset());
        assertThat(offset).isZero();
        DedupeRepo.Claim claim = STORE.transact("claim", tx -> tx.dedupe().claim(M, Source.GATEWAY, "evt_z", HASH, p, 0));
        assertThat(claim).as("the zombie's dedupe row rolled back with it").isEqualTo(DedupeRepo.Claim.FIRST);
    }

    @Test
    void theDatabaseRefusesToMoveALeaseBack() throws SQLException {
        int p = 8;
        long epoch = STORE.transact("assign", tx -> tx.partitions().acquire(p, "matcher-a.1")).epoch();
        STORE.transact("commit", tx -> {
            tx.partitions().commitOffset(p, epoch, 5);
            return null;
        });
        try (Connection c = PostgresFixture.app(URL); Statement st = c.createStatement()) {
            assertThatThrownBy(() -> st.executeUpdate("UPDATE partition_leases SET epoch = 0 WHERE partition = " + p))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot go back");
            c.rollback();
            assertThatThrownBy(() -> st.executeUpdate("UPDATE partition_leases SET next_offset = 1 WHERE partition = " + p))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cannot go back");
            c.rollback();
            assertThatThrownBy(() -> st.executeUpdate("DELETE FROM partition_leases WHERE partition = " + p))
                    .as("the application role has no DELETE").isInstanceOf(SQLException.class);
            c.rollback();
        }
    }

    @Test
    void quarantineRowsCannotBeChangedOrRemoved() throws SQLException {
        try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement()) {
            st.executeUpdate("""
                    INSERT INTO quarantine (id, partition, kafka_offset, merchant_id, source, event_key, payload_sha256, reason)
                    VALUES ('q1', 0, 1, 'm_fence', 'MERCHANT', 'k', repeat('a', 64), 'IDEMPOTENCY_CONFLICT')
                    """);
            assertThatThrownBy(() -> st.executeUpdate("UPDATE quarantine SET event_key = 'x' WHERE id = 'q1'"))
                    .isInstanceOf(SQLException.class);
            assertThatThrownBy(() -> st.executeUpdate("DELETE FROM quarantine WHERE id = 'q1'"))
                    .isInstanceOf(SQLException.class);
        }
    }

    /** Polls pg_stat_activity until the zombie's UPDATE is blocked on the new owner's row lock. */
    private static void awaitOneBackendWaitingOnALock() throws SQLException, InterruptedException {
        String database = URL.substring(URL.lastIndexOf('/') + 1).split("\\?")[0];
        try (Connection c = PostgresFixture.superuser(URL); PreparedStatement ps = c.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = ? AND wait_event_type = 'Lock'")) {
            ps.setString(1, database);
            for (int i = 0; i < 300; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    if (rs.getLong(1) == 1) {
                        return;
                    }
                }
                Thread.sleep(20);
            }
        }
        throw new AssertionError("the zombie's commit never waited on the new owner's lock");
    }
}
