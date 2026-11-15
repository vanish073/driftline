package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.DetectionStoreContract;

/** The watermark, timer, break and settlement contract against PostgreSQL, connected as the application role. */
class DetectionStoreContractIT extends DetectionStoreContract {

    private static final PostgresStore STORE =
            PostgresFixture.store(PostgresFixture.newDatabase("contract_detection"), BalanceStrategy.APPEND_SNAPSHOT, 4);

    @Override
    protected Store store() {
        return STORE;
    }
}
