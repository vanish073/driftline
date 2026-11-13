package io.github.vanish073.driftline.core.ledger;

import static io.github.vanish073.driftline.core.ledger.AccountCode.BANK;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_FEES;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_RECEIVABLE;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GST_INPUT_CREDIT;
import static io.github.vanish073.driftline.core.ledger.AccountCode.PAYMENTS_CLEARING;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SALES;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SUSPENSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Each posting rule of DESIGN §7.2: its cause and its entries. */
class PostingRulesTest {

    private static final MerchantId M = MerchantId.of("m_001");
    private static final PaymentId P = PaymentId.of("pay_1");
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00Z");

    private static Entry e(AccountCode account, long paise) {
        return new Entry(M, account, Money.ofPaise(paise));
    }

    private static Money p(long paise) {
        return Money.ofPaise(paise);
    }

    @Test
    void capture() {
        LedgerTxn txn = PostingRules.capture(M, P, p(49_900), AT);
        assertThat(txn.cause()).isEqualTo(new Cause(CauseType.CAPTURE, "pay_1"));
        assertThat(txn.entries()).containsExactly(e(GATEWAY_RECEIVABLE, 49_900), e(PAYMENTS_CLEARING, -49_900));
    }

    @Test
    void orderMatch() {
        LedgerTxn txn = PostingRules.orderMatch(M, P, p(49_900), AT);
        assertThat(txn.cause()).isEqualTo(new Cause(CauseType.ORDER_MATCH, "pay_1"));
        assertThat(txn.entries()).containsExactly(e(PAYMENTS_CLEARING, 49_900), e(SALES, -49_900));
    }

    @Test
    void refundDebitsSalesOnlyWhenThePaymentWasMatched() {
        assertThat(PostingRules.refund(M, P, p(500), false, AT).entries())
                .containsExactly(e(GATEWAY_RECEIVABLE, -500), e(PAYMENTS_CLEARING, 500));
        assertThat(PostingRules.refund(M, P, p(500), true, AT).entries())
                .containsExactly(e(GATEWAY_RECEIVABLE, -500), e(SALES, 500));
        assertThat(PostingRules.refund(M, P, p(500), true, AT).cause()).isEqualTo(new Cause(CauseType.REFUND, "pay_1"));
    }

    @Test
    void settlementPostsFeesAndGstAndMustAddUp() {
        LedgerTxn txn = PostingRules.settlement(M, "F-1", 3, p(97_640), p(2_000), p(360), p(100_000), AT);
        assertThat(txn.cause()).isEqualTo(new Cause(CauseType.SETTLEMENT, "F-1:3"));
        assertThat(txn.entries()).containsExactly(e(GATEWAY_RECEIVABLE, -100_000), e(BANK, 97_640),
                e(GATEWAY_FEES, 2_000), e(GST_INPUT_CREDIT, 360));
        assertThatThrownBy(() -> PostingRules.settlement(M, "F-1", 3, p(97_641), p(2_000), p(360), p(100_000), AT))
                .isInstanceOfSatisfying(LedgerRejected.class, r -> assertThat(r.reason()).isEqualTo(LedgerRejected.Reason.UNBALANCED));
    }

    @Test
    void zeroFeesAndGstAreLeftOut() {
        LedgerTxn txn = PostingRules.settlement(M, "F-1", 1, p(100), Money.ZERO, Money.ZERO, p(100), AT);
        assertThat(txn.entries()).containsExactly(e(GATEWAY_RECEIVABLE, -100), e(BANK, 100));
    }

    @Test
    void settlementPartialAndSuspense() {
        assertThat(PostingRules.settlementPartial(M, "F-1", 2, p(80), AT).entries())
                .containsExactly(e(GATEWAY_RECEIVABLE, -80), e(BANK, 80));
        assertThat(PostingRules.suspenseIn(M, "F-1", 4, p(700), AT).entries())
                .containsExactly(e(BANK, 700), e(SUSPENSE, -700));
        LedgerTxn release = PostingRules.suspenseRelease(M, "F-1", 4, p(700), p(20), p(4), p(724), AT);
        assertThat(release.cause()).isEqualTo(new Cause(CauseType.SUSPENSE_RELEASE, "F-1:4"));
        assertThat(release.entries()).containsExactly(e(GATEWAY_RECEIVABLE, -724), e(GATEWAY_FEES, 20),
                e(GST_INPUT_CREDIT, 4), e(SUSPENSE, 700));
    }

    @Test
    void manualAdjustmentMustBalance() {
        LedgerTxn txn = PostingRules.manualAdjustment(M, 12, List.of(e(BANK, -5), e(AccountCode.SETTLEMENT_DIFFERENCES, 5)), AT);
        assertThat(txn.cause()).isEqualTo(new Cause(CauseType.MANUAL_ADJUSTMENT, "12"));
        assertThatThrownBy(() -> PostingRules.manualAdjustment(M, 12, List.of(e(BANK, -5), e(SALES, 4)), AT))
                .isInstanceOf(LedgerRejected.class);
    }

    @Test
    void rejectsNonPositiveAmountsAndBadLineReferences() {
        assertThatThrownBy(() -> PostingRules.capture(M, P, Money.ZERO, AT)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PostingRules.settlementPartial(M, "F-1", 0, p(1), AT)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PostingRules.suspenseIn(M, "F 1", 1, p(1), AT)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PostingRules.settlement(M, "F-1", 1, p(100), p(-1), Money.ZERO, p(99), AT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
