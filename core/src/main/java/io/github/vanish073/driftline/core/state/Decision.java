package io.github.vanish073.driftline.core.state;

import java.util.Objects;

/**
 * The result of offering one event to a state machine: {@code Applied | IgnoredStale | IgnoredDuplicate | Conflict},
 * always with the rule that fired and the state after the decision.
 *
 * @param <S> the state type
 */
public sealed interface Decision<S> {

    /** The state after the decision. Unchanged unless the decision is {@link Applied} or a payment conflict flag. */
    S state();

    Rule rule();

    DecisionKind kind();

    static <S> Decision<S> of(Rule rule, S state) {
        return switch (rule.kind()) {
            case APPLIED -> new Applied<>(state, rule);
            case IGNORED_STALE -> new IgnoredStale<>(state, rule);
            case IGNORED_DUPLICATE -> new IgnoredDuplicate<>(state, rule);
            case CONFLICT -> new Conflict<>(state, rule);
        };
    }

    record Applied<S>(S state, Rule rule) implements Decision<S> {
        public Applied {
            check(state, rule, DecisionKind.APPLIED);
        }

        @Override
        public DecisionKind kind() {
            return DecisionKind.APPLIED;
        }
    }

    record IgnoredStale<S>(S state, Rule rule) implements Decision<S> {
        public IgnoredStale {
            check(state, rule, DecisionKind.IGNORED_STALE);
        }

        @Override
        public DecisionKind kind() {
            return DecisionKind.IGNORED_STALE;
        }
    }

    record IgnoredDuplicate<S>(S state, Rule rule) implements Decision<S> {
        public IgnoredDuplicate {
            check(state, rule, DecisionKind.IGNORED_DUPLICATE);
        }

        @Override
        public DecisionKind kind() {
            return DecisionKind.IGNORED_DUPLICATE;
        }
    }

    record Conflict<S>(S state, Rule rule) implements Decision<S> {
        public Conflict {
            check(state, rule, DecisionKind.CONFLICT);
        }

        @Override
        public DecisionKind kind() {
            return DecisionKind.CONFLICT;
        }
    }

    private static void check(Object state, Rule rule, DecisionKind expected) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(rule, "rule");
        if (rule.kind() != expected) {
            throw new IllegalArgumentException("rule " + rule.code() + " is " + rule.kind() + ", not " + expected);
        }
    }
}
