package io.github.vanish073.driftline.core.ledger;

/** A ledger transaction that breaks the double-entry rules (DESIGN §7.3). */
public final class LedgerRejected extends IllegalArgumentException {

    /** The rule that was broken. */
    public enum Reason {
        TOO_FEW_ENTRIES,
        ZERO_ENTRY,
        MIXED_MERCHANTS,
        OVERFLOW,
        UNBALANCED
    }

    private final Reason reason;

    public LedgerRejected(Reason reason, String message) {
        super(reason + ": " + message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
