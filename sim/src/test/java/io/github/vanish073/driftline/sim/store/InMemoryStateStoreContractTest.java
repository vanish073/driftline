package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.StateStoreContract;

/** The dedupe, payment, order and partition contract, run against the simulator's store. */
class InMemoryStateStoreContractTest extends StateStoreContract {

    @Override
    protected Store store() {
        return new InMemoryStore();
    }
}
