package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.LedgerStoreContract;

/** The shared Store contract against PostgreSQL with ROW_LOCK balances. */
class RowLockStoreContractIT extends LedgerStoreContract {

    private static final PostgresStore STORE =
            PostgresFixture.store(PostgresFixture.newDatabase("contract_row_lock"), BalanceStrategy.ROW_LOCK, 4);

    @Override
    protected Store store() {
        return STORE;
    }
}
