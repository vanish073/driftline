package io.github.vanish073.driftline.sim;

import java.util.Locale;

/** Run sizes (DESIGN §11.15). Shift 3 has {@code calm}; {@code pr} and {@code nightly} arrive with faults in Shift 4. */
public enum Profile {
    /** Baseline, no faults: 2 merchants, 2 partitions, about 80 orders, 1 traffic day, drained for 7 days. */
    CALM(2, 2, 20, 70, 90, 1, 7);

    public final int merchants;
    public final int partitions;
    public final int customers;
    public final int minOrders;
    public final int maxOrders;
    public final int trafficDays;
    public final int drainDays;

    Profile(int merchants, int partitions, int customers, int minOrders, int maxOrders, int trafficDays, int drainDays) {
        this.merchants = merchants;
        this.partitions = partitions;
        this.customers = customers;
        this.minOrders = minOrders;
        this.maxOrders = maxOrders;
        this.trafficDays = trafficDays;
        this.drainDays = drainDays;
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
        throw new IllegalArgumentException("unknown profile " + id + "; Shift 3 has: calm");
    }
}
