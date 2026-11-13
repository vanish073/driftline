package io.github.vanish073.driftline.core.order;

import static io.github.vanish073.driftline.core.order.OrderStatus.CANCELLED;
import static io.github.vanish073.driftline.core.order.OrderStatus.CREATED;
import static io.github.vanish073.driftline.core.order.OrderStatus.EXPIRED;
import static io.github.vanish073.driftline.core.order.OrderStatus.FULFILLED;
import static io.github.vanish073.driftline.core.order.OrderStatus.PAID;
import static io.github.vanish073.driftline.core.order.OrderStatus.PENDING_PAYMENT;

import java.util.List;
import java.util.Objects;

/**
 * The order transition table, DESIGN §4.4 as data: the legal graph, and the version rules that use it (refined by
 * D25). {@link OrderStateMachine} only computes the key and looks the row up.
 */
public final class OrderTransitionTable {

    /** A legal one-step transition of the merchant's order. */
    public record Edge(OrderStatus from, OrderStatus to) {

        public Edge {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
        }
    }

    /** How the incoming version compares with the current one. */
    public enum VersionOrder {
        /** The order is unknown (NONE). */
        FIRST,
        OLDER,
        SAME,
        NEWER
    }

    /** What the machine found when it compared the incoming snapshot with the current state. */
    public enum Fit {
        /** No comparison is needed for this version order. */
        NOT_CHECKED,
        SAME_PAYLOAD,
        DIFFERENT_PAYLOAD,
        /** The incoming state cannot be reached from the current state in the graph. */
        UNREACHABLE,
        /** Reachable, but the snapshot names a different payment than the one the order was paid with. */
        PAYMENT_CHANGED,
        REACHABLE
    }

    /** One rule of the version table. */
    public record Row(VersionOrder order, Fit fit, OrderRule rule) {

        public Row {
            Objects.requireNonNull(order, "order");
            Objects.requireNonNull(fit, "fit");
            Objects.requireNonNull(rule, "rule");
        }
    }

    /** DESIGN §4.4: CREATED → PENDING_PAYMENT → PAID → FULFILLED; CREATED or PENDING_PAYMENT → EXPIRED or CANCELLED. */
    private static final List<Edge> EDGES = List.of(
            new Edge(CREATED, PENDING_PAYMENT),
            new Edge(PENDING_PAYMENT, PAID),
            new Edge(PAID, FULFILLED),
            new Edge(CREATED, EXPIRED),
            new Edge(CREATED, CANCELLED),
            new Edge(PENDING_PAYMENT, EXPIRED),
            new Edge(PENDING_PAYMENT, CANCELLED));

    private static final List<Row> ROWS = List.of(
            new Row(VersionOrder.FIRST, Fit.NOT_CHECKED, OrderRule.FIRST_EVIDENCE),
            new Row(VersionOrder.OLDER, Fit.NOT_CHECKED, OrderRule.STALE_VERSION),
            new Row(VersionOrder.SAME, Fit.SAME_PAYLOAD, OrderRule.SAME_VERSION_REPEATED),
            new Row(VersionOrder.SAME, Fit.DIFFERENT_PAYLOAD, OrderRule.SAME_VERSION_DIFFERENT),
            new Row(VersionOrder.NEWER, Fit.UNREACHABLE, OrderRule.ILLEGAL_TARGET),
            new Row(VersionOrder.NEWER, Fit.PAYMENT_CHANGED, OrderRule.PAID_WITH_CHANGED),
            new Row(VersionOrder.NEWER, Fit.REACHABLE, OrderRule.NEWER_VERSION));

    private OrderTransitionTable() {
    }

    public static List<Edge> edges() {
        return EDGES;
    }

    public static List<Row> rows() {
        return ROWS;
    }

    /**
     * True if {@code to} can be reached from {@code from} in zero or more steps. Zero steps counts (D25): a higher
     * version in the same state is an update, not an anomaly. NONE is not a node of the graph.
     */
    public static boolean reachable(OrderStatus from, OrderStatus to) {
        if (from == OrderStatus.NONE || to == OrderStatus.NONE) {
            throw new IllegalArgumentException("NONE is not part of the order graph");
        }
        if (from == to) {
            return true;
        }
        for (Edge edge : EDGES) {
            if (edge.from() == from && reachable(edge.to(), to)) {
                return true;
            }
        }
        return false;
    }

    public static Row lookup(VersionOrder order, Fit fit) {
        for (Row row : ROWS) {
            if (row.order() == order && row.fit() == fit) {
                return row;
            }
        }
        throw new IllegalStateException("no order rule for " + order + " with " + fit);
    }
}
