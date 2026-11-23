package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
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
import java.util.TreeSet;

/**
 * A merchant's order system (DESIGN §11.4): orders with versions and expiry, reported to Driftline as snapshots, each
 * version retried with the same {@code Idempotency-Key} until it is acknowledged.
 *
 * <p>Under MERCHANT_KEY_REUSE the merchant runs a buggy client: a new version is sometimes sent under the previous
 * version's key, with the new body. Those keys are recorded, because only they may ever reach the quarantine.
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
        String lastKey;
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
    private final FaultInjector faults;
    private final TreeSet<String> reusedKeys = new TreeSet<>();
    private final TreeMap<String, TreeMap<String, String>> confirms = new TreeMap<>();

    public FakeMerchant(MerchantId merchantId, Scheduler scheduler, SimNetwork network, Ingress ingress, Trace trace,
                        FaultInjector faults) {
        this.merchantId = merchantId;
        this.faults = faults;
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

    /**
     * The confirm API (DESIGN §8.2, §11.4): Driftline asks the merchant to accept a late success. ACCEPTED only while the
     * order still awaits payment, has not expired and has this amount: then the order is paid with the payment and
     * fulfilled; REJECTED otherwise (an expired order is never fulfilled late). Idempotent by key, as a merchant's API
     * would be: a repeated key gets the first answer.
     */
    public TreeMap<String, String> confirm(CallRequest.MerchantConfirm c, Optional<String> key) {
        if (key.isPresent() && confirms.containsKey(key.get())) {
            return new TreeMap<>(confirms.get(key.get()));
        }
        Order order = orders.get(c.orderId());
        boolean awaiting = order != null
                && (order.status == EventType.ORDER_CREATED || order.status == EventType.ORDER_PENDING_PAYMENT);
        boolean paidWithIt = order != null && order.paidWith.filter(c.paymentId()::equals).isPresent();
        TreeMap<String, String> answer = new TreeMap<>();
        if (paidWithIt) {
            answer.put("status", "ACCEPTED");   // already paid with this very payment: confirming it again is a no-op
        } else if (awaiting && scheduler.instant().isBefore(order.expiresAt) && order.amount.equals(c.amount())) {
            advance(order, EventType.ORDER_PAID, Optional.of(c.paymentId()));
            scheduler.after(FULFIL_AFTER_MILLIS, node, () -> advance(order, EventType.ORDER_FULFILLED, order.paidWith));
            answer.put("status", "ACCEPTED");
        } else {
            answer.put("status", "REJECTED");
        }
        trace.line(scheduler.now(), node, "confirm " + c.orderId() + " with " + c.paymentId() + ": " + answer.get("status"));
        key.ifPresent(k -> confirms.put(k, new TreeMap<>(answer)));
        return answer;
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
        boolean reuse = faults.reuseKey(order.lastKey != null);
        if (reuse) {
            key = order.lastKey;
            reusedKeys.add(key);
        }
        order.lastKey = key;
        trace.line(scheduler.now(), node, status.wireName() + " " + order.id + " v" + order.version
                + (reuse ? " REUSING key " + key : ""));
        String sentKey = key;
        Delivery delivery = new Delivery(scheduler, node, (attemptNo, answered, reply) -> network.send("ingest",
                () -> ingress.receive(new Request.OrderEvent(merchantId, body, Optional.of(sentKey)), status2 -> {
                    answered.accept(status2);
                    network.send(node, () -> reply.accept(status2));
                })), at -> null);
        deliveries.add(delivery);
        delivery.start();
    }

    public Optional<Order> order(OrderId id) {
        return Optional.ofNullable(orders.get(id));
    }

    /** Deliveries Driftline never accepted (D87), including those given up on. */
    public long undelivered() {
        return deliveries.stream().filter(d -> !d.accepted()).count();
    }

    /** Keys this merchant's buggy client sent with a second, different body. */
    public TreeSet<String> reusedKeys() {
        return new TreeSet<>(reusedKeys);
    }
}
