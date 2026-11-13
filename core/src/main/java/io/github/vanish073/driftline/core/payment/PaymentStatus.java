package io.github.vanish073.driftline.core.payment;

/**
 * The gateway status of a payment as Driftline knows it (DESIGN §4.3). Refunds are tracked separately; they are
 * not part of this table.
 */
public enum PaymentStatus {
    /** No event seen yet. */
    NONE,
    PENDING,
    SUCCEEDED,
    FAILED
}
