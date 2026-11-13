package io.github.vanish073.driftline.core.port;

/**
 * Settlement files, their lines, matches and ambiguity witnesses. Mirrors {@code settlement_files}, {@code settlement_lines}, {@code settlement_matches} and {@code ambiguity_witnesses} in DESIGN §12.
 *
 * <p>Empty until the shift that needs it. Every method added here lands in {@code InMemoryStore} and
 * {@code PostgresStore} in the same change, with a contract test (DESIGN §3.2, CLAUDE.md "Store parity").
 */
public interface SettlementRepo {
}
