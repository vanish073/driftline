package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.testkit.store.DetectionStoreContract;

/** The watermark, timer, break and settlement contract, run against the simulator's store. */
class InMemoryDetectionStoreContractTest extends DetectionStoreContract {

    @Override
    protected Store store() {
        return new InMemoryStore();
    }
}
