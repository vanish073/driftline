package io.github.vanish073.driftline.sim.node;

/** Thrown inside a store transaction to kill a node before its commit (NODE_CRASH mid-batch; crash point CP6). */
final class SimCrash extends RuntimeException {

    SimCrash() {
        super("crashed before commit", null, false, false);
    }
}
