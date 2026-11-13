package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Ledger layer 3 under real concurrency (DESIGN §7.4, §11.9): 16 threads post the same cause at once. */
class LedgerConcurrencyIT {

    private static final String URL = PostgresFixture.newDatabase("ledger_concurrency");
    private static final int THREADS = 16;
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00Z");

    private static List<PostResult> race(PostgresStore store, IntFunction<LedgerTxn> txnForThread) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<PostResult>> futures = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                LedgerTxn txn = txnForThread.apply(i);
                futures.add(pool.submit(() -> {
                    start.await();
                    return store.transact("race", tx -> tx.ledger().post(txn));
                }));
            }
            start.countDown();
            List<PostResult> results = new ArrayList<>();
            for (Future<PostResult> f : futures) {
                results.add(f.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    private static long rows(String sql) throws SQLException {
        try (Connection c = PostgresFixture.superuser(URL); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @ParameterizedTest
    @EnumSource(BalanceStrategy.class)
    void sixteenIdenticalPostsRecordOneTransaction(BalanceStrategy strategy) throws Exception {
        MerchantId m = MerchantId.of("m-same-" + strategy);
        LedgerTxn capture = PostingRules.capture(m, PaymentId.of("pay_1"), Money.ofPaise(49_900), AT);
        try (PostgresStore store = PostgresFixture.store(URL, strategy, THREADS)) {
            List<PostResult> results = race(store, i -> capture);

            assertThat(results).filteredOn(PostResult.Posted.class::isInstance).hasSize(1);
            assertThat(results).filteredOn(PostResult.Duplicate.class::isInstance).hasSize(THREADS - 1);
            assertThat(rows("SELECT count(*) FROM ledger_transactions WHERE merchant_id = '" + m + "'")).isEqualTo(1);
            assertThat(rows("SELECT count(*) FROM ledger_entries e JOIN ledger_transactions t USING (txn_id)"
                    + " WHERE t.merchant_id = '" + m + "'")).isEqualTo(2);
            Money receivable = store.transact("read", tx -> tx.ledger().balance(m, AccountCode.GATEWAY_RECEIVABLE));
            assertThat(receivable).isEqualTo(Money.ofPaise(49_900));
        }
    }

    @ParameterizedTest
    @EnumSource(BalanceStrategy.class)
    void sixteenDifferentPostsOfOneCauseRecordOneAndReportTheRestAsConflicts(BalanceStrategy strategy) throws Exception {
        MerchantId m = MerchantId.of("m-diff-" + strategy);
        try (PostgresStore store = PostgresFixture.store(URL, strategy, THREADS)) {
            List<PostResult> results = race(store,
                    i -> PostingRules.capture(m, PaymentId.of("pay_1"), Money.ofPaise(1_000 + i), AT));

            assertThat(results).filteredOn(PostResult.Posted.class::isInstance).hasSize(1);
            assertThat(results).filteredOn(PostResult.LedgerConflict.class::isInstance).hasSize(THREADS - 1);
            Money receivable = store.transact("read", tx -> tx.ledger().balance(m, AccountCode.GATEWAY_RECEIVABLE));
            Money clearing = store.transact("read", tx -> tx.ledger().balance(m, AccountCode.PAYMENTS_CLEARING));
            assertThat(receivable.plus(clearing)).isEqualTo(Money.ZERO);
            assertThat(receivable.paise()).isBetween(1_000L, 1_000L + THREADS - 1);
        }
    }
}
