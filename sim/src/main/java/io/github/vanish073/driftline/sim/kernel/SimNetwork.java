package io.github.vanish073.driftline.sim.kernel;

import io.github.vanish073.driftline.sim.random.SplitMix64;

/**
 * The network between simulated nodes (DESIGN §11.3). Shift 3: delivery only, after a delay drawn from the network
 * stream. Drops, duplicates, heavy-tailed delays and partitions arrive with the fault injector in Shift 4.
 */
public final class SimNetwork {

    public static final long MIN_DELAY_MILLIS = 5;
    public static final long MAX_DELAY_MILLIS = 200;

    private final Scheduler scheduler;
    private final SplitMix64 rng;
    private long delivered;

    public SimNetwork(Scheduler scheduler, SplitMix64 rng) {
        this.scheduler = scheduler;
        this.rng = rng;
    }

    /** Delivers to node {@code to} after a network delay. */
    public void send(String to, Runnable delivery) {
        long delay = rng.between(MIN_DELAY_MILLIS, MAX_DELAY_MILLIS);
        scheduler.after(delay, to, () -> {
            delivered++;
            delivery.run();
        });
    }

    public long delivered() {
        return delivered;
    }
}
