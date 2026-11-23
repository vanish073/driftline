package io.github.vanish073.driftline.sim;

import java.util.Locale;

/**
 * Run sizes (DESIGN §11.15). A profile gives ranges; each seed draws its own shape from the workload stream.
 */
public enum Profile {
    /** Baseline, no faults: 2 merchants, 2 partitions, about 80 orders, 1 traffic day, drained for 7 days. */
    CALM(2, 2, 2, 2, 20, 70, 90, 1, 7, false),
    /** Every pull request, 10,000 seeds: 2–3 merchants, 1–3 partitions, 60–150 orders over 2 days, swarm faults. */
    PR(2, 3, 1, 3, 30, 60, 150, 2, 7, true),
    /**
     * The long campaign (D85): 5 merchants on 6 partitions (one stays idle), 1,800–2,200 orders from 500 customers
     * over 5 days, drained for 7, swarm faults.
     */
    NIGHTLY(5, 5, 6, 6, 500, 1_800, 2_200, 5, 7, true);

    public final int minMerchants;
    public final int maxMerchants;
    public final int minPartitions;
    public final int maxPartitions;
    public final int customers;
    public final int minOrders;
    public final int maxOrders;
    public final int trafficDays;
    public final int drainDays;
    /** Whether seeds draw a swarm vector of faults (§11.5); {@code calm} runs fault-free. */
    public final boolean faults;

    Profile(int minMerchants, int maxMerchants, int minPartitions, int maxPartitions, int customers, int minOrders,
            int maxOrders, int trafficDays, int drainDays, boolean faults) {
        this.minMerchants = minMerchants;
        this.maxMerchants = maxMerchants;
        this.minPartitions = minPartitions;
        this.maxPartitions = maxPartitions;
        this.customers = customers;
        this.minOrders = minOrders;
        this.maxOrders = maxOrders;
        this.trafficDays = trafficDays;
        this.drainDays = drainDays;
        this.faults = faults;
    }

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Profile fromId(String id) {
        for (Profile p : values()) {
            if (p.id().equals(id)) {
                return p;
            }
        }
        throw new IllegalArgumentException("unknown profile " + id + "; known: calm, pr, nightly");
    }
}
