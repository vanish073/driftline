package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The sequence-gap trap of DESIGN §7.5. A slow transaction takes the lower entry ids and commits after a faster one
 * with higher ids. A snapshot that folds "every visible entry up to the highest id" skips the slow entries for good;
 * the {@code xid8} horizon does not.
 */
class SnapshotHorizonIT {

    private static final String URL = PostgresFixture.newDatabase("snapshot_horizon");
    private static final MerchantId M = MerchantId.of("m-horizon");
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00Z");

    private static long query(String sql) throws SQLException {
        try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String entriesOf(String payment) {
        return "SELECT %s(e.entry_id) FROM ledger_entries e JOIN ledger_transactions t USING (txn_id)"
                + " WHERE t.merchant_id = 'm-horizon' AND t.cause_id = '" + payment + "'";
    }

    @Test
    void aSnapshotNeverSkipsAnEntryThatCommitsLate() throws Exception {
        try (PostgresStore store = PostgresFixture.store(URL, BalanceStrategy.APPEND_SNAPSHOT, 4)) {
            // Open the merchant's accounts first. Accounts are created on first posting, so without this the fast
            // transaction would wait for the slow one's uncommitted account rows and the race could not happen.
            store.transact("open", tx -> tx.ledger().post(
                    PostingRules.capture(M, PaymentId.of("pay_open"), Money.ofPaise(1), AT)));

            CountDownLatch slowHasWritten = new CountDownLatch(1);
            CountDownLatch letSlowCommit = new CountDownLatch(1);

            // The slow transaction posts first, so its entries get the lower ids, then waits before committing.
            CompletableFuture<Void> slow = CompletableFuture.runAsync(() -> store.transact("slow", tx -> {
                tx.ledger().post(PostingRules.capture(M, PaymentId.of("pay_slow"), Money.ofPaise(700), AT));
                slowHasWritten.countDown();
                try {
                    assertThat(letSlowCommit.await(30, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return null;
            }));
            assertThat(slowHasWritten.await(30, TimeUnit.SECONDS)).isTrue();

            // A fast transaction posts and commits with higher ids; then the snapshotter runs.
            store.transact("fast", tx -> tx.ledger().post(
                    PostingRules.capture(M, PaymentId.of("pay_fast"), Money.ofPaise(50), AT)));
            long highestVisibleIdAtSnapshot = query(entriesOf("pay_fast").formatted("max"));
            store.snapshotBalances();

            letSlowCommit.countDown();
            slow.get(30, TimeUnit.SECONDS);

            // The trap is real: the slow entries have lower ids than the fast ones, yet committed later.
            assertThat(query(entriesOf("pay_slow").formatted("max"))).isLessThan(query(entriesOf("pay_fast").formatted("min")));
            long naiveBalance = query("""
                    SELECT coalesce(sum(e.amount_paise), 0)
                      FROM ledger_entries e JOIN ledger_accounts a USING (account_id)
                      JOIN ledger_transactions t USING (txn_id)
                     WHERE a.merchant_id = 'm-horizon' AND a.code = 'GATEWAY_RECEIVABLE'
                       AND (t.cause_id IN ('pay_open', 'pay_fast') OR e.entry_id > %d)
                    """.formatted(highestVisibleIdAtSnapshot));
            assertThat(naiveBalance).as("folding by entry id would lose the slow posting").isEqualTo(51);

            // The xid8 horizon keeps it: both postings count, before and after another snapshot.
            Money before = store.transact("read", tx -> tx.ledger().balance(M, AccountCode.GATEWAY_RECEIVABLE));
            assertThat(before).isEqualTo(Money.ofPaise(751));
            store.snapshotBalances();
            Money after = store.transact("read", tx -> tx.ledger().balance(M, AccountCode.GATEWAY_RECEIVABLE));
            assertThat(after).isEqualTo(Money.ofPaise(751));
            assertThat(query("SELECT count(*) FROM account_snapshots s JOIN ledger_accounts a USING (account_id)"
                    + " WHERE a.merchant_id = 'm-horizon'")).isPositive();
        }
    }
}
