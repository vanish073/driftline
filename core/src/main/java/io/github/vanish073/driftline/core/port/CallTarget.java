package io.github.vanish073.driftline.core.port;

/** Who an {@link OutboundCall} goes to (DESIGN §9.1). */
public enum CallTarget {
    /** Status queries, refunds and refund lookups. */
    GATEWAY,
    /** The merchant's confirm callback. */
    MERCHANT,
    /** Driftline's own observation endpoint, which publishes to Kafka (D10). */
    DRIFTLINE,
    /** The ops escalation sink. */
    OPS
}
