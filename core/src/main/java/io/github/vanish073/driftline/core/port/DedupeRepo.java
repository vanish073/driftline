package io.github.vanish073.driftline.core.port;

/**
 * Dedupe rows keyed by {@code (merchant, source, eventKey)}, written in the same transaction as the state change (D3). Mirrors {@code processed_events} in DESIGN §12.
 *
 * <p>Empty until the shift that needs it. Every method added here lands in {@code InMemoryStore} and
 * {@code PostgresStore} in the same change, with a contract test (DESIGN §3.2, CLAUDE.md "Store parity").
 */
public interface DedupeRepo {
}
