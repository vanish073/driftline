package io.github.vanish073.driftline.sim.invariant;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostingRules;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderRule;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentRule;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Each invariant check fires on a bad row and stays quiet on a good one. */
class ChecksTest {

    private static final MerchantId M = MerchantId.of("m_0");
    private static final PaymentId P = PaymentId.of("pay_1");
    private static final Instant T = Instant.parse("2026-11-13T06:00:00Z");

    @Test
    void ledgerBalanced() {
        assertThat(Checks.ledgerBalanced(PostingRules.capture(M, P, Money.ofPaise(5), T))).isEmpty();
        LedgerTxn bad = LedgerTxn.unchecked(M, new Cause(CauseType.CAPTURE, "pay_1"), T,
                List.of(new Entry(M, AccountCode.BANK, Money.ofPaise(5)), new Entry(M, AccountCode.SALES, Money.ofPaise(-4))));
        assertThat(Checks.ledgerBalanced(bad)).hasValueSatisfying(s -> assertThat(s).contains("UNBALANCED"));
    }

    @Test
    void paymentTransitionsMustBeInTheTable() {
        assertThat(Checks.paymentTransitionLegal(new PaymentTransition(P, PaymentStatus.FAILED, PaymentStatus.SUCCEEDED,
                PaymentRule.LATE_SUCCESS, "e", T))).isEmpty();
        assertThat(Checks.paymentTransitionLegal(new PaymentTransition(P, PaymentStatus.SUCCEEDED, PaymentStatus.FAILED,
                PaymentRule.FAILURE_LATER, "e", T))).isPresent();
        assertThat(Checks.paymentTransitionLegal(new PaymentTransition(P, PaymentStatus.PENDING, PaymentStatus.SUCCEEDED,
                PaymentRule.FACTS_MISMATCH, "e", T))).isPresent();
    }

    @Test
    void orderTransitionsMustFollowTheGraph() {
        OrderId o = OrderId.of("ord_1");
        assertThat(Checks.orderTransitionLegal(new OrderTransition(o, OrderStatus.CREATED, OrderStatus.PAID, 3,
                OrderRule.NEWER_VERSION, "k", T))).isEmpty();
        assertThat(Checks.orderTransitionLegal(new OrderTransition(o, OrderStatus.EXPIRED, OrderStatus.PAID, 3,
                OrderRule.NEWER_VERSION, "k", T))).isPresent();
        assertThat(Checks.orderTransitionLegal(new OrderTransition(o, OrderStatus.CREATED, OrderStatus.PAID, 3,
                OrderRule.STALE_VERSION, "k", T))).isPresent();
    }

    @Test
    void aPaymentNeverLeavesSucceeded() {
        PaymentTransition toFailed = new PaymentTransition(P, PaymentStatus.SUCCEEDED, PaymentStatus.FAILED,
                PaymentRule.FAILURE_LATER, "e", T);
        assertThat(Checks.oneFinalState(PaymentStatus.SUCCEEDED, toFailed)).isPresent();
        PaymentTransition sneaky = new PaymentTransition(P, PaymentStatus.FAILED, PaymentStatus.SUCCEEDED,
                PaymentRule.SUCCESS_EARLIER, "e", T);
        assertThat(Checks.oneFinalState(PaymentStatus.FAILED, sneaky)).isPresent();
        assertThat(Checks.oneFinalState(PaymentStatus.FAILED, new PaymentTransition(P, PaymentStatus.FAILED,
                PaymentStatus.SUCCEEDED, PaymentRule.LATE_SUCCESS, "e", T))).isEmpty();
    }
}
