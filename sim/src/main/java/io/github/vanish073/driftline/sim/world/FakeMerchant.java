package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A merchant's order system (DESIGN §11.4): orders with versions and expiry, reported to Driftline as snapshots, each
 * version retried with the same {@code Idempotency-Key} until it is acknowledged.
 */
public final class FakeMerchant {

    /** How long an order waits for payment before it expires. */
    public static final long EXPIRY_MILLIS = 15 * 60_000;
    /** How long after payment the merchant fulfils. */
    static final long FULFIL_AFTER_MILLIS = 60_000;

    /** The merchant's own record of an order: its system of record. */
    public static final class Order {
        final OrderId id;
        final Money amount;
        final Instant expiresAt;
        EventType status;
        long version;
        Optional<PaymentId> paidWith = Optional.empty();

        Order(OrderId id, Money amount, Instant expiresAt) {
            this.id = id;
            this.amount = amount;
            this.expiresAt = expiresAt;
        }

        public EventType status() {
            return status;
        }

        public Optional<PaymentId> paidWith() {
            return paidWith;
        }
    }

    private final MerchantId merchantId;
    private final Scheduler scheduler;
    private final SimNetwork network;
    private final Ingress ingress;
    private final Trace trace;
    private final String node;
    private final TreeMap<OrderId, Order> orders = new TreeMap<>();
    private final List<Delivery> deliveries = new ArrayList<>();

    public FakeMerchant(MerchantId merchantId, Scheduler scheduler, SimNetwork network, Ingress ingress, Trace trace) {
        this.merchantId = merchantId;
        this.scheduler = scheduler;
        this.network = network;
        this.ingress = ingress;
        this.trace = trace;
        this.node = "merchant:" + merchantId;
    }

    public MerchantId merchantId() {
        return merchantId;
    }

    /** The customer starts checkout: CREATED now, PENDING_PAYMENT once the payment page opens, expiry later. */
    public void createOrder(OrderId id, Money amount, boolean awaitsPayment) {
        Order order = new Order(id, amount, scheduler.instant().plusMillis(EXPIRY_MILLIS));
        orders.put(id, order);
        advance(order, EventType.ORDER_CREATED, Optional.empty());
        if (awaitsPayment) {
            advance(order, EventType.ORDER_PENDING_PAYMENT, Optional.empty());
        }
        scheduler.after(EXPIRY_MILLIS, node, () -> expireIfUnpaid(order));
    }

    /** The gateway's redirect or callback reached the merchant. */
    public void onPaymentResult(OrderId id, PaymentId payment, boolean succeeded) {
        Order order = orders.get(id);
        if (order == null || !succeeded) {
            return;
        }
        boolean awaiting = order.status == EventType.ORDER_CREATED || order.status == EventType.ORDER_PENDING_PAYMENT;
        if (awaiting && scheduler.instant().isBefore(order.expiresAt)) {
            advance(order, EventType.ORDER_PAID, Optional.of(payment));
            scheduler.after(FULFIL_AFTER_MILLIS, node, () -> advance(order, EventType.ORDER_FULFILLED, order.paidWith));
        } else {
            trace.line(scheduler.now(), node, "ignores payment " + payment + " for " + id + " in " + order.status.wireName());
        }
    }

    private void expireIfUnpaid(Order order) {
        if (order.status == EventType.ORDER_CREATED || order.status == EventType.ORDER_PENDING_PAYMENT) {
            advance(order, EventType.ORDER_EXPIRED, Optional.empty());
        }
    }

    private void advance(Order order, EventType status, Optional<PaymentId> paidWith) {
        order.status = status;
        order.version++;
        order.paidWith = paidWith;
        OrderEventMessage message = new OrderEventMessage(status, scheduler.instant(),
                new OrderPayload(order.id, order.version, order.amount, order.expiresAt, paidWith));
        byte[] body = OrderEventCodec.encode(message);
        String key = DerivedId.of("order-event", merchantId.value(), order.id.value(), Long.toString(order.version)).value();
        trace.line(scheduler.now(), node, status.wireName() + " " + order.id + " v" + order.version);
        Delivery delivery = new Delivery(scheduler, node, (attemptNo, reply) -> network.send("ingest",
                () -> ingress.receive(new Request.OrderEvent(merchantId, body, Optional.of(key)),
                        status2 -> network.send(node, () -> reply.accept(status2)))), at -> null);
        deliveries.add(delivery);
        delivery.start();
    }

    public Optional<Order> order(OrderId id) {
        return Optional.ofNullable(orders.get(id));
    }

    /** Deliveries not yet acknowledged and not given up on. */
    public long undelivered() {
        return deliveries.stream().filter(d -> !d.acked() && !d.gaveUp()).count();
    }
}
