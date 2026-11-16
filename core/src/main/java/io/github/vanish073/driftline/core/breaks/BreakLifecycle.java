package io.github.vanish073.driftline.core.breaks;

import java.util.List;

/**
 * The break lifecycle of DESIGN §8.1 as a data table: which state may follow which. Every transition the matcher or the
 * resolver writes must be a row here; the simulator checks each committed transition against it
 * (INV-TRANSITIONS-LEGAL).
 *
 * <pre>
 * DETECTED ──> AWAITING_EVIDENCE ──> ACTION_PENDING ──> RESOLVED_AUTO
 *    │               │                     │
 *    │               └──> SELF_HEALED      └──> ESCALATED ──> RESOLVED_MANUAL
 *    └──> LINKED (follows the payment's action-owner break)
 * any non-final state ──(now ≥ escalate_by)──> ESCALATED
 * </pre>
 *
 * Beyond the diagram (D60): DETECTED → RESOLVED_AUTO (the matcher auto-resolves an itemised short line, §6.5);
 * DETECTED → SELF_HEALED (the late evidence arrived before the resolver first looked); AWAITING_EVIDENCE →
 * RESOLVED_AUTO (a merchant accepted a late success and its {@code order.paid} arrived); and ACTION_PENDING →
 * AWAITING_EVIDENCE (a refund lookup must be answered before anything else is sent). There is deliberately no way from
 * ACTION_PENDING to SELF_HEALED: once a refund may be in flight, a late {@code order.paid} is escalated, never
 * quietly healed.
 */
public final class BreakLifecycle {

    /** One allowed move. */
    public record Move(BreakState from, BreakState to) {
    }

    public static final List<Move> MOVES = List.of(
            new Move(BreakState.DETECTED, BreakState.AWAITING_EVIDENCE),
            new Move(BreakState.DETECTED, BreakState.ACTION_PENDING),
            new Move(BreakState.DETECTED, BreakState.RESOLVED_AUTO),
            new Move(BreakState.DETECTED, BreakState.SELF_HEALED),
            new Move(BreakState.DETECTED, BreakState.LINKED),
            new Move(BreakState.DETECTED, BreakState.ESCALATED),
            new Move(BreakState.AWAITING_EVIDENCE, BreakState.ACTION_PENDING),
            new Move(BreakState.AWAITING_EVIDENCE, BreakState.RESOLVED_AUTO),
            new Move(BreakState.AWAITING_EVIDENCE, BreakState.SELF_HEALED),
            new Move(BreakState.AWAITING_EVIDENCE, BreakState.ESCALATED),
            new Move(BreakState.ACTION_PENDING, BreakState.AWAITING_EVIDENCE),
            new Move(BreakState.ACTION_PENDING, BreakState.RESOLVED_AUTO),
            new Move(BreakState.ACTION_PENDING, BreakState.ESCALATED),
            new Move(BreakState.ESCALATED, BreakState.RESOLVED_MANUAL));

    /** States nothing leaves: the break is closed. */
    public static final List<BreakState> FINAL = List.of(BreakState.RESOLVED_AUTO, BreakState.SELF_HEALED,
            BreakState.RESOLVED_MANUAL, BreakState.LINKED);

    private BreakLifecycle() {
    }

    public static boolean allowed(BreakState from, BreakState to) {
        return MOVES.contains(new Move(from, to));
    }

    public static boolean isFinal(BreakState state) {
        return FINAL.contains(state);
    }

    /**
     * Whether a break in this state counts as handled for INV-DEADLINE: closed, or handed to a person. A LINKED break
     * follows its payment's action owner, so it is handled when that one is.
     */
    public static boolean isHandled(BreakState state) {
        return isFinal(state) || state == BreakState.ESCALATED;
    }
}
