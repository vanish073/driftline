package io.github.vanish073.driftline.sim.invariant;

import io.github.vanish073.driftline.core.ledger.LedgerChecks;
import io.github.vanish073.driftline.core.ledger.LedgerRejected;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.order.OrderStatus;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.order.OrderTransitionTable;
import io.github.vanish073.driftline.core.payment.PaymentRule;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.payment.PaymentTransitionTable;
import io.github.vanish073.driftline.core.state.DecisionKind;
import java.util.Optional;

/** The per-row checks behind the invariants, as pure functions so a test can feed them bad rows directly. */
public final class Checks {

    private Checks() {
    }

    /** INV-LEDGER-BALANCED: sums to 0, at least 2 entries, no zero entry, one merchant. */
    public static Optional<String> ledgerBalanced(LedgerTxn txn) {
        try {
            LedgerChecks.requireValid(txn.merchantId(), txn.entries());
            return Optional.empty();
        } catch (LedgerRejected rejected) {
            return Optional.of(txn.cause() + " of " + txn.merchantId() + ": " + rejected.getMessage());
        }
    }

    /** INV-TRANSITIONS-LEGAL for a payment: the rule exists in the table with these statuses (§4.3, D24). */
    public static Optional<String> paymentTransitionLegal(PaymentTransition t) {
        boolean guard = t.rule() == PaymentRule.FACTS_MISMATCH || t.rule() == PaymentRule.ALREADY_IN_CONFLICT;
        boolean legal = guard
                ? t.from() == t.to()
                : PaymentTransitionTable.rows().stream()
                        .anyMatch(r -> r.rule() == t.rule() && r.from() == t.from() && r.to() == t.to());
        return legal ? Optional.empty()
                : Optional.of(t.paymentId() + " " + t.from() + " -> " + t.to() + " by " + t.rule() + " is not in the table");
    }

    /** INV-TRANSITIONS-LEGAL for an order: an applied change follows the graph; anything else changes nothing (§4.4). */
    public static Optional<String> orderTransitionLegal(OrderTransition t) {
        boolean legal = t.rule().kind() == DecisionKind.APPLIED
                ? t.from() == OrderStatus.NONE || OrderTransitionTable.reachable(t.from(), t.to())
                : t.from() == t.to();
        return legal ? Optional.empty()
                : Optional.of(t.orderId() + " " + t.from() + " -> " + t.to() + " by " + t.rule() + " is not legal");
    }

    /**
     * INV-ONE-OUTCOME as far as Shift 3 can see it: once SUCCEEDED, a payment stays SUCCEEDED, and FAILED becomes
     * SUCCEEDED only through LATE_SUCCESS. The outcome field itself arrives with resolution (Shifts 5 and 8).
     */
    public static Optional<String> oneFinalState(PaymentStatus before, PaymentTransition t) {
        if (before == PaymentStatus.SUCCEEDED && t.to() != PaymentStatus.SUCCEEDED) {
            return Optional.of(t.paymentId() + " left SUCCEEDED for " + t.to() + " by " + t.rule());
        }
        if (before == PaymentStatus.FAILED && t.to() == PaymentStatus.SUCCEEDED && t.rule() != PaymentRule.LATE_SUCCESS) {
            return Optional.of(t.paymentId() + " went FAILED -> SUCCEEDED by " + t.rule());
        }
        return Optional.empty();
    }
}
