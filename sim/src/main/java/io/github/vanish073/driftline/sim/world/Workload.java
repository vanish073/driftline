package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Generates the orders of a run from the workload stream (DESIGN §11.4). */
public final class Workload {

    /** Weights out of 100, in {@link Scenario} order. */
    private static final List<Integer> WEIGHTS = List.of(61, 8, 6, 6, 5, 5, 3, 3, 3);

    private Workload() {
    }

    public static List<PlannedOrder> generate(SplitMix64 rng, List<MerchantId> merchants, int customers, int orders,
                                              long trafficMillis) {
        List<PlannedOrder> planned = new ArrayList<>();
        for (int i = 0; i < orders; i++) {
            MerchantId merchant = rng.pick(merchants);
            String customer = "customer:" + rng.nextLong(customers);
            long createdAt = rng.nextLong(trafficMillis);
            Money amount = Money.ofPaise(rng.between(1_000L, 500_000L));
            planned.add(new PlannedOrder(merchant, OrderId.of("ord_" + i), customer, amount, createdAt, scenario(rng)));
        }
        planned.sort(Comparator.comparingLong(PlannedOrder::createdAtMillis).thenComparing(PlannedOrder::orderId));
        return List.copyOf(planned);
    }

    private static Scenario scenario(SplitMix64 rng) {
        long roll = rng.nextLong(100);
        Scenario[] all = Scenario.values();
        for (int i = 0; i < all.length; i++) {
            roll -= WEIGHTS.get(i);
            if (roll < 0) {
                return all[i];
            }
        }
        return Scenario.NORMAL;
    }
}
