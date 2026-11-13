package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.testkit.store.LedgerStoreContract;

/**
 * The shared Store contract against PostgreSQL with APPEND_SNAPSHOT balances. The snapshotter runs after every
 * transaction, so each balance read goes through the snapshot-plus-delta path, not only the plain sum.
 */
class AppendSnapshotStoreContractIT extends LedgerStoreContract {

    private static final PostgresStore STORE = PostgresFixture.store(
            PostgresFixture.newDatabase("contract_append_snapshot"), BalanceStrategy.APPEND_SNAPSHOT, 4);

    private static final Store SNAPSHOT_AFTER_EVERY_TRANSACTION = new Store() {
        @Override
        public <R> R transact(String txName, TxBody<R> body) {
            try {
                return STORE.transact(txName, body);
            } finally {
                STORE.snapshotBalances();
            }
        }
    };

    @Override
    protected Store store() {
        return SNAPSHOT_AFTER_EVERY_TRANSACTION;
    }
}
