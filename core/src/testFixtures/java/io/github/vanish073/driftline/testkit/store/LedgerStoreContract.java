package io.github.vanish073.driftline.testkit.store;

import static io.github.vanish073.driftline.core.ledger.AccountCode.BANK;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_FEES;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_RECEIVABLE;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GST_INPUT_CREDIT;
import static io.github.vanish073.driftline.core.ledger.AccountCode.PAYMENTS_CLEARING;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SALES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerRejected;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The ledger part of the Store contract (DESIGN §3.2, §11.9). The same tests run against {@code InMemoryStore} and
 * against {@code PostgresStore} with each balance strategy, so the simulator's store cannot drift from the real one.
 *
 * <p>Implementations may share one store between tests: every test works on two merchants of its own, derived from
 * the test class and name.
 */
public abstract class LedgerStoreContract {

    private static final Instant AT = Instant.parse("2026-11-13T06:00:00.123456Z");
    private static final PaymentId P1 = PaymentId.of("pay_1");
    private static final PaymentId P2 = PaymentId.of("pay_2");

    private Store store;
    private MerchantId m;
    private MerchantId other;

    /** The store under test. It may be shared across tests. */
    protected abstract Store store();

    @BeforeEach
    void freshMerchants(TestInfo test) {
        store = store();
        String name = getClass().getName() + "#" + test.getDisplayName();
        m = MerchantId.of("m-" + DerivedId.of("contract", name, "a"));
        other = MerchantId.of("m-" + DerivedId.of("contract", name, "b"));
    }

    private <R> R tx(TxBody<R> body) {
        return store.transact("contract", body);
    }

    private PostResult post(LedgerTxn txn) {
        return tx(t -> t.ledger().post(txn));
    }

    private Optional<LedgerTxn> find(MerchantId merchant, Cause cause) {
        return tx(t -> t.ledger().find(merchant, cause));
    }

    private long balance(MerchantId merchant, AccountCode account) {
        return tx(t -> t.ledger().balance(merchant, account)).paise();
    }

    @Test
    void aPostedTransactionCanBeFoundAndMovesBalances() {
        LedgerTxn capture = PostingRules.capture(m, P1, Money.ofPaise(49_900), AT);

        assertThat(post(capture)).isEqualTo(new PostResult.Posted(capture.id()));

        assertThat(find(m, capture.cause())).contains(capture);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isEqualTo(49_900);
        assertThat(balance(m, PAYMENTS_CLEARING)).isEqualTo(-49_900);
        assertThat(balance(m, BANK)).isZero();
    }

    @Test
    void anUnknownCauseAndAnUntouchedAccountAreEmpty() {
        assertThat(find(m, new Cause(CauseType.CAPTURE, "pay_none"))).isEmpty();
        assertThat(balance(m, SALES)).isZero();
    }

    @Test
    void repostingTheSameCauseIsANoOp() {
        LedgerTxn capture = PostingRules.capture(m, P1, Money.ofPaise(49_900), AT);
        post(capture);

        LedgerTxn replayLater = PostingRules.capture(m, P1, Money.ofPaise(49_900), AT.plusSeconds(3_600));
        assertThat(post(replayLater)).isEqualTo(new PostResult.Duplicate(capture.id()));

        assertThat(find(m, capture.cause())).contains(capture);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isEqualTo(49_900);
    }

    @Test
    void theSameCauseWithDifferentEntriesIsALedgerConflict() {
        LedgerTxn capture = PostingRules.capture(m, P1, Money.ofPaise(49_900), AT);
        post(capture);

        LedgerTxn different = PostingRules.capture(m, P1, Money.ofPaise(50_000), AT);
        PostResult result = post(different);

        assertThat(result).isInstanceOf(PostResult.LedgerConflict.class);
        PostResult.LedgerConflict conflict = (PostResult.LedgerConflict) result;
        assertThat(conflict.existing()).isEqualTo(capture);
        assertThat(conflict.attempted()).isEqualTo(different);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isEqualTo(49_900);
    }

    @Test
    void causesAreUniquePerMerchantOnly() {
        assertThat(post(PostingRules.capture(m, P1, Money.ofPaise(100), AT))).isInstanceOf(PostResult.Posted.class);
        assertThat(post(PostingRules.capture(other, P1, Money.ofPaise(200), AT))).isInstanceOf(PostResult.Posted.class);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isEqualTo(100);
        assertThat(balance(other, GATEWAY_RECEIVABLE)).isEqualTo(200);
    }

    @Test
    void aTransactionReadsItsOwnWritesAndSeesItsOwnDuplicate() {
        LedgerTxn capture = PostingRules.capture(m, P1, Money.ofPaise(49_900), AT);
        List<Object> seen = tx(t -> List.of(
                t.ledger().post(capture),
                t.ledger().balance(m, GATEWAY_RECEIVABLE),
                t.ledger().post(capture),
                t.ledger().find(m, capture.cause()).isPresent()));
        assertThat(seen).containsExactly(new PostResult.Posted(capture.id()), Money.ofPaise(49_900),
                new PostResult.Duplicate(capture.id()), true);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isEqualTo(49_900);
    }

    @Test
    void aTransactionThatThrowsLeavesNoTrace() {
        LedgerTxn capture = PostingRules.capture(m, P1, Money.ofPaise(49_900), AT);
        assertThatThrownBy(() -> tx(t -> {
            t.ledger().post(capture);
            throw new IllegalStateException("crash after posting, before commit");
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("crash");

        assertThat(find(m, capture.cause())).isEmpty();
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isZero();
        assertThat(post(capture)).isInstanceOf(PostResult.Posted.class);
    }

    /** Layer 2 of DESIGN §7.3: the store rejects a bad transaction even when layer 1 was skipped. */
    @Test
    void theStoreRejectsABadTransactionByItself() {
        Cause cause = new Cause(CauseType.MANUAL_ADJUSTMENT, "1");
        List<List<Entry>> bad = List.of(
                List.of(Entry.debit(m, BANK, Money.ofPaise(100)), Entry.credit(m, SALES, Money.ofPaise(90))),
                List.of(Entry.debit(m, BANK, Money.ofPaise(100))),
                List.of(Entry.debit(m, BANK, Money.ofPaise(100)), Entry.credit(m, SALES, Money.ofPaise(100)),
                        new Entry(m, GATEWAY_FEES, Money.ZERO)),
                List.of(Entry.debit(m, BANK, Money.ofPaise(100)), Entry.credit(other, SALES, Money.ofPaise(100))));
        List<LedgerRejected.Reason> reasons = List.of(LedgerRejected.Reason.UNBALANCED,
                LedgerRejected.Reason.TOO_FEW_ENTRIES, LedgerRejected.Reason.ZERO_ENTRY,
                LedgerRejected.Reason.MIXED_MERCHANTS);
        for (int i = 0; i < bad.size(); i++) {
            LedgerTxn unchecked = LedgerTxn.unchecked(m, cause, AT, bad.get(i));
            LedgerRejected.Reason expected = reasons.get(i);
            assertThatThrownBy(() -> post(unchecked))
                    .isInstanceOfSatisfying(LedgerRejected.class, e -> assertThat(e.reason()).isEqualTo(expected));
        }
        assertThat(find(m, cause)).isEmpty();
        assertThat(balance(m, BANK)).isZero();
    }

    @Test
    void aPaymentLifecycleKeepsTheBooksBalanced() {
        Money gross = Money.ofPaise(100_000);
        Money fee = Money.ofPaise(2_000);
        Money gst = Money.ofPaise(360);
        Money received = gross.minus(fee).minus(gst);
        post(PostingRules.capture(m, P1, gross, AT));
        post(PostingRules.orderMatch(m, P1, gross, AT));
        post(PostingRules.settlement(m, "F-20261114", 1, received, fee, gst, gross, AT));
        post(PostingRules.capture(m, P2, Money.ofPaise(5_000), AT));
        post(PostingRules.refund(m, P2, Money.ofPaise(5_000), false, AT));

        assertThat(balance(m, BANK)).isEqualTo(received.paise());
        assertThat(balance(m, GATEWAY_FEES)).isEqualTo(2_000);
        assertThat(balance(m, GST_INPUT_CREDIT)).isEqualTo(360);
        assertThat(balance(m, SALES)).isEqualTo(-100_000);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isZero();
        assertThat(balance(m, PAYMENTS_CLEARING)).isZero();
        long total = 0;
        for (AccountCode code : AccountCode.values()) {
            total = Math.addExact(total, balance(m, code));
        }
        assertThat(total).as("the books of one merchant sum to zero").isZero();
    }

    @Test
    void aTxCannotBeUsedAfterItsTransactionEnded() {
        AtomicReference<Tx> leaked = new AtomicReference<>();
        tx(t -> {
            leaked.set(t);
            return null;
        });
        assertThatThrownBy(() -> leaked.get().ledger().balance(m, BANK)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> leaked.get().ledger().post(PostingRules.capture(m, P1, Money.ofPaise(1), AT)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(balance(m, GATEWAY_RECEIVABLE)).isZero();
    }
}
