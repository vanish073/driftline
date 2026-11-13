package io.github.vanish073.driftline.core.state;

/**
 * A named rule of a transition table. Every decision carries the rule that produced it, and the rule id is written
 * to the transition log ({@code payment_transitions}, {@code order_transitions}; DESIGN §4.3).
 */
public interface Rule {

    /** A stable id such as {@code "P05"}, used in logs and the transition tables. */
    String code();

    DecisionKind kind();

    /** One line in plain words. */
    String summary();
}
