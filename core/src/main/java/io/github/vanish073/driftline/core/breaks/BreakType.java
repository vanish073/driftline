package io.github.vanish073.driftline.core.breaks;

import java.util.List;

/**
 * Every kind of finding Driftline records (DESIGN §5.6, §8.2): the six breaks of the brief, the anomalies that are
 * always escalated (D14), and operational alerts such as a stalled settlement feed (§5.4).
 */
public enum BreakType {
    DEBITED_NO_ORDER(BreakKind.BREAK),
    LATE_SUCCESS(BreakKind.BREAK),
    DUPLICATE_CAPTURE(BreakKind.BREAK),
    SHORT_SETTLEMENT(BreakKind.BREAK),
    MISSING_SETTLEMENT(BreakKind.BREAK),
    ORPHAN_SETTLEMENT(BreakKind.BREAK),

    /** A payment's gateway records contradict each other (a CONFLICT decision of §4.3); the payment is frozen. */
    STATUS_CONFLICT(BreakKind.ANOMALY),
    /** A merchant order history that contradicts itself (§4.4). */
    ORDER_ANOMALY(BreakKind.ANOMALY),
    /** A referenced line that paid more than the expected net (§6.5). */
    OVER_SETTLEMENT(BreakKind.ANOMALY),
    /** A second, different file for a settlement date that already has one (§6.2). */
    FILE_CONFLICT(BreakKind.ANOMALY),
    /** A line that references a payment already settled by another line (§6.3). */
    SETTLEMENT_CONFLICT(BreakKind.ANOMALY),
    INFERRED_MATCH_CHALLENGED(BreakKind.ANOMALY),
    IDEMPOTENCY_CONFLICT(BreakKind.ANOMALY),
    LEDGER_CONFLICT(BreakKind.ANOMALY),

    /** No complete settlement file for an expected date long after it was due (§5.4). Payments are not flagged. */
    FEED_STALL(BreakKind.ALERT);

    /** The refund-capable breaks, in the precedence order of the action-owner rule (§5.6, D11). */
    public static final List<BreakType> REFUND_CAPABLE = List.of(DUPLICATE_CAPTURE, LATE_SUCCESS, DEBITED_NO_ORDER);

    private final BreakKind kind;

    BreakType(BreakKind kind) {
        this.kind = kind;
    }

    public BreakKind kind() {
        return kind;
    }

    public boolean refundCapable() {
        return REFUND_CAPABLE.contains(this);
    }
}
