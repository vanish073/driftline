package io.github.vanish073.driftline.core.port;

/**
 * The typed body of an {@link OutboundCall}. It becomes a sealed interface in Shift 8, when the command kinds of
 * DESIGN §9.1 land with the dispatcher (D27).
 */
public interface CallRequest {
}
