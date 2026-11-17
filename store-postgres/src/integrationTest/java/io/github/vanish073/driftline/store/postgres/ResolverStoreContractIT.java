package io.github.vanish073.driftline.store.postgres;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.ResolverStoreContract;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The plan, outbox and audit contract against PostgreSQL, connected as the application role. The review queue and the
 * outbox claim span every merchant, so each test gets a database of its own.
 */
class ResolverStoreContractIT extends ResolverStoreContract {

    private static final AtomicInteger DATABASES = new AtomicInteger();

    @Override
    protected Store store() {
        return PostgresFixture.store(PostgresFixture.newDatabase("contract_resolver_" + DATABASES.incrementAndGet()),
                BalanceStrategy.APPEND_SNAPSHOT, 2);
    }
}
