package io.github.vanish073.driftline.core.port;

/**
 * Breaks and their append-only transition log. Mirrors {@code breaks} and {@code break_transitions} in DESIGN §12.
 *
 * <p>Empty until the shift that needs it. Every method added here lands in {@code InMemoryStore} and
 * {@code PostgresStore} in the same change, with a contract test (DESIGN §3.2, CLAUDE.md "Store parity").
 */
public interface BreakRepo {
}
