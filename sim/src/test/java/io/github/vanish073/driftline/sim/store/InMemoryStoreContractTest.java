package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.LedgerStoreContract;

/** The shared Store contract, run against the simulator's store. */
class InMemoryStoreContractTest extends LedgerStoreContract {

    @Override
    protected Store store() {
        return new InMemoryStore();
    }
}
