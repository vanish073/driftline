package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.StateStoreContract;

/** The dedupe, payment, order and partition contract against PostgreSQL, connected as the application role. */
class StateStoreContractIT extends StateStoreContract {

    private static final PostgresStore STORE =
            PostgresFixture.store(PostgresFixture.newDatabase("contract_state"), BalanceStrategy.APPEND_SNAPSHOT, 4);

    @Override
    protected Store store() {
        return STORE;
    }
}
