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
    DEBITED_NO_ORDER
}
