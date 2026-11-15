package io.github.vanish073.driftline.sim.kernel;

import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.random.SplitMix64;

/**
 * The network between simulated nodes (DESIGN §11.3). The base delay comes from the {@code network} stream. The
 * fault injector may drop a message, deliver it twice, or add a heavy-tailed delay that reorders it (NET_DROP,
 * NET_DUP, NET_DELAY, §11.5). After HEAL every message arrives once, after the base delay.
 */
public final class SimNetwork {

    public static final long MIN_DELAY_MILLIS = 5;
    public static final long MAX_DELAY_MILLIS = 200;

    private final Scheduler scheduler;
    private final SplitMix64 rng;
    private final FaultInjector faults;
    private long delivered;

    public SimNetwork(Scheduler scheduler, SplitMix64 rng, FaultInjector faults) {
        this.scheduler = scheduler;
        this.rng = rng;
        this.faults = faults;
    }

    /** Delivers to node {@code to} after a network delay, unless a fault intervenes. */
    public void send(String to, Runnable delivery) {
        long delay = rng.between(MIN_DELAY_MILLIS, MAX_DELAY_MILLIS);
        long secondDelay = rng.between(MIN_DELAY_MILLIS, MAX_DELAY_MILLIS * 10);
        long extra = faults.extraDelay();
        boolean dropped = faults.drop();
        boolean twice = faults.duplicate();
        if (dropped) {
            return;
        }
        deliver(to, delay + extra, delivery);
        if (twice) {
            deliver(to, delay + secondDelay, delivery);
        }
    }

    private void deliver(String to, long delay, Runnable delivery) {
        scheduler.after(delay, to, () -> {
            delivered++;
            delivery.run();
        });
    }

    public long delivered() {
        return delivered;
    }
}
