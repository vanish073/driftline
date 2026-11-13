package io.github.vanish073.driftline.store.postgres;

/** How account balances are kept (DESIGN §7.5). Both are measured in Shift 10. */
public enum BalanceStrategy {
    /**
     * Every posting updates one {@code account_balances} row per account, so all writers on a hot account run one
     * after another, each waiting for the previous commit. The measured baseline.
     */
    ROW_LOCK,
    /**
     * Postings only insert entries. A balance is the latest snapshot plus every entry at or above its {@code xid8}
     * horizon; {@link PostgresStore#snapshotBalances()} folds finished entries into new snapshots. The default (D9).
     */
    APPEND_SNAPSHOT
}
