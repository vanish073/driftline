package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderTransition;
import java.util.List;
import java.util.Optional;

/** Merchant orders and their append-only transition log. Mirrors {@code orders} and {@code order_transitions} (§12). */
public interface OrderRepo {

    Optional<OrderState> find(MerchantId merchantId, OrderId orderId);

    /** Inserts or replaces the state of {@code state.orderId()}. */
    void save(MerchantId merchantId, OrderState state);

    /** Appends one decision of the order state machine, in order. */
    void appendTransition(MerchantId merchantId, OrderTransition transition);

    /** The transitions of one order, oldest first. */
    List<OrderTransition> transitions(MerchantId merchantId, OrderId orderId);
}
