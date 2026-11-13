package io.github.vanish073.driftline.sim.archfixture;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.sim.runner.archfixture.DriverPart;

/** Test fixture: a "kernel" class in the sim package. It may build canaries, but must not use the runner. */
public final class KernelPart {

    public Canaries canaries() {
        return Canaries.of(Canary.NO_FENCING);
    }

    public DriverPart driver() {
        return new DriverPart();
    }
}
