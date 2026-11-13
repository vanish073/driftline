package io.github.vanish073.driftline.sim.archfixture;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;

/** Test fixture: a sim class that builds canaries and touches nothing else. */
public final class CanaryOnlyPart {

    public Canaries canaries() {
        return Canaries.of(Canary.DEDUPE_OFF);
    }
}
