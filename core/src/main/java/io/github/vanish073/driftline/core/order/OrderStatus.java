package io.github.vanish073.driftline.core.order;

/** The merchant's order states (DESIGN §4.4), plus {@link #NONE} for an order Driftline has not seen yet. */
public enum OrderStatus {
    NONE,
    CREATED,
    PENDING_PAYMENT,
    PAID,
    FULFILLED,
    EXPIRED,
    CANCELLED;

    /** True for PAID and FULFILLED, whose snapshot names the payment the order was paid with. */
    public boolean carriesPayment() {
        return this == PAID || this == FULFILLED;
    }

    /** FULFILLED, EXPIRED and CANCELLED have no outgoing edges. */
    public boolean isTerminal() {
        return this == FULFILLED || this == EXPIRED || this == CANCELLED;
    }
}
