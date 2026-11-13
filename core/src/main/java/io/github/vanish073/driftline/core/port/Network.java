package io.github.vanish073.driftline.core.port;

/**
 * Outbound calls (DESIGN §3.2). Non-blocking: the result arrives later as an input to the sending node, or never.
 * The caller decides timeouts using its {@link Clock}. Production wires {@code HttpNetwork}; the simulator wires
 * {@code SimNetwork}, where the scheduler decides delivery, drop, duplication and delay.
 */
public interface Network {

    void send(OutboundCall call);
}
