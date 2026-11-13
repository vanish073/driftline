package io.github.vanish073.driftline.store.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterContainer;

/**
 * DESIGN §7.5: ROW_LOCK and APPEND_SNAPSHOT give the same balances. Random balanced postings (some of them replays)
 * go to one store of each strategy; snapshots are taken at random points; every balance must equal an oracle that
 * sums the accepted entries directly.
 */
class BalanceStrategyEquivalenceIT {

    private static final PostgresStore ROW_LOCK = PostgresFixture.store(
            PostgresFixture.newDatabase("equivalence_row_lock"), BalanceStrategy.ROW_LOCK, 2);
    private static final PostgresStore APPEND_SNAPSHOT = PostgresFixture.store(
            PostgresFixture.newDatabase("equivalence_append_snapshot"), BalanceStrategy.APPEND_SNAPSHOT, 2);
    private static final AtomicInteger TRY = new AtomicInteger();
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00Z");

    /** One step: a balanced adjustment (debits on some accounts, one balancing credit), or a snapshot. */
    record Step(int merchant, long auditSeq, List<Long> debits, int creditAccount, boolean snapshotAfter) {
    }

    @AfterContainer
    static void close() {
        ROW_LOCK.close();
        APPEND_SNAPSHOT.close();
    }

    @Property(tries = 25)
    void bothStrategiesAgreeWithTheOracle(@ForAll("steps") List<Step> steps) {
        String run = "t" + TRY.incrementAndGet();
        List<MerchantId> merchants = List.of(MerchantId.of(run + "-a"), MerchantId.of(run + "-b"));
        Map<String, Long> oracle = new TreeMap<>();
        AccountCode[] codes = AccountCode.values();

        for (Step step : steps) {
            MerchantId m = merchants.get(step.merchant());
            List<Entry> entries = new ArrayList<>();
            long total = 0;
            for (int i = 0; i < step.debits().size(); i++) {
                AccountCode code = codes[i];
                if (code.ordinal() == step.creditAccount()) {
                    continue;
                }
                entries.add(new Entry(m, code, Money.ofPaise(step.debits().get(i))));
                total += step.debits().get(i);
            }
            if (entries.isEmpty()) {
                continue;
            }
            entries.add(new Entry(m, codes[step.creditAccount()], Money.ofPaise(-total)));
            LedgerTxn txn = PostingRules.manualAdjustment(m, step.auditSeq(), entries, AT);

            PostResult a = ROW_LOCK.transact("post", tx -> tx.ledger().post(txn));
            PostResult b = APPEND_SNAPSHOT.transact("post", tx -> tx.ledger().post(txn));
            assertThat(b.getClass()).isEqualTo(a.getClass());
            if (a instanceof PostResult.Posted) {
                for (Entry e : txn.entries()) {
                    oracle.merge(m + "/" + e.account(), e.amount().paise(), Math::addExact);
                }
            }
            if (step.snapshotAfter()) {
                APPEND_SNAPSHOT.snapshotBalances();
            }
        }

        for (MerchantId m : merchants) {
            for (AccountCode code : codes) {
                long expected = oracle.getOrDefault(m + "/" + code, 0L);
                Money rowLock = ROW_LOCK.transact("read", tx -> tx.ledger().balance(m, code));
                Money appendSnapshot = APPEND_SNAPSHOT.transact("read", tx -> tx.ledger().balance(m, code));
                assertThat(rowLock.paise()).as("ROW_LOCK %s %s", m, code).isEqualTo(expected);
                assertThat(appendSnapshot.paise()).as("APPEND_SNAPSHOT %s %s", m, code).isEqualTo(expected);
            }
        }
    }

    @Provide
    Arbitrary<List<Step>> steps() {
        Arbitrary<Step> step = Combinators.combine(
                Arbitraries.integers().between(0, 1),
                Arbitraries.longs().between(1, 6),          // few audit numbers, so replays and conflicts happen
                Arbitraries.longs().between(1, 5_000_000).list().ofMinSize(1).ofMaxSize(4),
                Arbitraries.integers().between(0, AccountCode.values().length - 1),
                Arbitraries.integers().between(0, 3).map(i -> i == 0))
                .as(Step::new);
        return step.list().ofMinSize(1).ofMaxSize(15);
    }
}
