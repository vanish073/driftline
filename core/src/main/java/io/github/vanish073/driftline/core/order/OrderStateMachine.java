package io.github.vanish073.driftline.core.order;

import io.github.vanish073.driftline.core.order.OrderTransitionTable.Fit;
import io.github.vanish073.driftline.core.order.OrderTransitionTable.VersionOrder;
import io.github.vanish073.driftline.core.state.Decision;
import io.github.vanish073.driftline.core.state.DecisionKind;

/**
 * Decides what one merchant order event does to an order (DESIGN §4.4, D25). Pure and stateless. A CONFLICT leaves
 * the state unchanged; the caller raises ORDER_ANOMALY.
 */
public final class OrderStateMachine {

    public Decision<OrderState> decide(OrderState current, OrderEvidence incoming) {
        if (!current.orderId().equals(incoming.orderId())) {
            throw new IllegalArgumentException("evidence for order " + incoming.orderId()
                    + " offered to order " + current.orderId());
        }
        VersionOrder order = current.status() == OrderStatus.NONE ? VersionOrder.FIRST
                : incoming.version() < current.version() ? VersionOrder.OLDER
                : incoming.version() == current.version() ? VersionOrder.SAME
                : VersionOrder.NEWER;
        Fit fit = switch (order) {
            case FIRST, OLDER -> Fit.NOT_CHECKED;
            case SAME -> incoming.toState().equals(current) ? Fit.SAME_PAYLOAD : Fit.DIFFERENT_PAYLOAD;
            case NEWER -> !OrderTransitionTable.reachable(current.status(), incoming.reported()) ? Fit.UNREACHABLE
                    : paidWithChanged(current, incoming) ? Fit.PAYMENT_CHANGED
                    : Fit.REACHABLE;
        };
        OrderRule rule = OrderTransitionTable.lookup(order, fit).rule();
        return Decision.of(rule, rule.kind() == DecisionKind.APPLIED ? incoming.toState() : current);
    }

    private static boolean paidWithChanged(OrderState current, OrderEvidence incoming) {
        return current.paidWith().isPresent()
                && incoming.paidWith().isPresent()
                && !current.paidWith().equals(incoming.paidWith());
    }
}
