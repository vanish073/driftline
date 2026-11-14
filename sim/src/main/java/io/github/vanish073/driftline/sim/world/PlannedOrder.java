package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.money.Money;

/** One order the workload will play out, with its scenario label. */
public record PlannedOrder(MerchantId merchantId, OrderId orderId, String customer, Money amount, long createdAtMillis,
                           Scenario scenario) {
}
