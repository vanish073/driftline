package io.github.vanish073.driftline.core.breaks;

/**
 * The break lifecycle of DESIGN §8.1. The matcher creates breaks in {@link #DETECTED}, links later refund-capable
 * breaks of a payment to its action owner ({@link #LINKED}, D11), and auto-resolves a short settlement that the
 * itemised fee and tax explain ({@link #RESOLVED_AUTO}, §6.5). Every other move belongs to the resolver (Shift 8).
 */
public enum BreakState {
    DETECTED,
    AWAITING_EVIDENCE,
    ACTION_PENDING,
    RESOLVED_AUTO,
    SELF_HEALED,
    ESCALATED,
    RESOLVED_MANUAL,
    LINKED;

    public boolean isFinal() {
        return this == RESOLVED_AUTO || this == SELF_HEALED || this == RESOLVED_MANUAL;
    }
}
