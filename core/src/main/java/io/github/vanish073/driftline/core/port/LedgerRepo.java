package io.github.vanish073.driftline.core.port;

/**
 * The double-entry ledger: append-only, every transaction sums to zero. Mirrors {@code ledger_accounts}, {@code ledger_transactions}, {@code ledger_entries} and the balance tables in DESIGN §12.
 *
 * <p>Empty until the shift that needs it. Every method added here lands in {@code InMemoryStore} and
 * {@code PostgresStore} in the same change, with a contract test (DESIGN §3.2, CLAUDE.md "Store parity").
 */
public interface LedgerRepo {
}
