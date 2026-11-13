package io.github.vanish073.driftline.core.order;

import io.github.vanish073.driftline.core.state.DecisionKind;
import io.github.vanish073.driftline.core.state.Rule;

/** The rules of the order transition table (DESIGN §4.4 as refined by D25). Every CONFLICT raises ORDER_ANOMALY. */
public enum OrderRule implements Rule {
    FIRST_EVIDENCE("O01", DecisionKind.APPLIED, "first event for an order Driftline has not seen"),
    NEWER_VERSION("O02", DecisionKind.APPLIED,
            "higher version whose state is reachable from the current one (skipped versions allowed)"),
    STALE_VERSION("O03", DecisionKind.IGNORED_STALE, "lower version than the current one"),
    SAME_VERSION_REPEATED("O04", DecisionKind.IGNORED_DUPLICATE, "same version with the same payload"),
    SAME_VERSION_DIFFERENT("O05", DecisionKind.CONFLICT, "same version with a different payload: ORDER_ANOMALY"),
    ILLEGAL_TARGET("O06", DecisionKind.CONFLICT, "higher version whose state is not reachable: ORDER_ANOMALY"),
    PAID_WITH_CHANGED("O07", DecisionKind.CONFLICT,
            "higher version naming a different payment than the one the order was paid with: ORDER_ANOMALY");

    private final String code;
    private final DecisionKind kind;
    private final String summary;

    OrderRule(String code, DecisionKind kind, String summary) {
        this.code = code;
        this.kind = kind;
        this.summary = summary;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public DecisionKind kind() {
        return kind;
    }

    @Override
    public String summary() {
        return summary;
    }
}
