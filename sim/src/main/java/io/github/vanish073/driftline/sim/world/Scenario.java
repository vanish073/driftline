package io.github.vanish073.driftline.sim.world;

/**
 * The labelled situations the workload creates (DESIGN §11.4). Labels feed the per-type numbers; correctness checks
 * never rely on them, because faults create situations nobody planned (§11.8).
 */
public enum Scenario {
    /** Paid, confirmed to the merchant, fulfilled. */
    NORMAL,
    /** The payment fails and the order expires. */
    PAYMENT_FAILED,
    /** An order with no payment at all; it expires. */
    ABANDONED,
    /** A failure first, then a success for the same payment (LATE_SUCCESS). */
    LATE_SUCCESS,
    /** Two successful payments for one order (DUPLICATE_CAPTURE). */
    DUPLICATE_CAPTURE,
    /** The customer is debited but the merchant never hears of it; the order expires (DEBITED_NO_ORDER). */
    DEBITED_NO_ORDER,
    /** A normal order whose settlement line pays less than the expected net (SHORT_SETTLEMENT). */
    SHORT_SETTLEMENT,
    /** A normal order whose payment the bank never settles (MISSING_SETTLEMENT). */
    MISSING_SETTLEMENT,
    /** A normal order whose settlement file also carries an extra line with an unknown reference (ORPHAN_SETTLEMENT). */
    ORPHAN_SETTLEMENT
}
