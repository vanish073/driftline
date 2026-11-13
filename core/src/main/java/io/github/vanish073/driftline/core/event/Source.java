package io.github.vanish073.driftline.core.event;

/** Where an event came from (DESIGN §4.2). */
public enum Source {
    /** A payment-gateway webhook. */
    GATEWAY,
    /** A merchant order event. */
    MERCHANT,
    /** A line or trailer of a bank settlement file. */
    BANK,
    /** A fact Driftline learned by asking, such as a status re-query or a refund result (D10). */
    OBSERVATION
}
