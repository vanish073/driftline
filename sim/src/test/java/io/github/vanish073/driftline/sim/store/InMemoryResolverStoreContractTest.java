package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.ResolverStoreContract;

/** The plan, outbox and audit contract, run against the simulator's store. */
class InMemoryResolverStoreContractTest extends ResolverStoreContract {

    @Override
    protected Store store() {
        return new InMemoryStore();
    }
}
