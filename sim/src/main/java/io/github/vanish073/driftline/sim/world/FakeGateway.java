package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The payment gateway (DESIGN §11.4): it holds payments and their status, moves the customer's money into its float on
 * success, sends signed webhooks on a retry schedule (each attempt re-signed with a fresh timestamp), answers status
 * queries, and settles each day's captures to the merchant net of fees and GST. Settlement files reach Driftline from
 * Shift 5; here the money moves and the lines are recorded.
 */
public final class FakeGateway {

    public static final String FLOAT = "gateway:float";
    public static final String REVENUE = "gateway:revenue";
    /** 2% fee and 18% GST on the fee, per payment, rounded half up (the configured model, DESIGN §6.4). */
    static final long FEE_BPS = 200;
    static final long GST_BPS = 1_800;

    /** The gateway's own record of a payment. */
    public static final class Payment {
        final MerchantId merchant;
        final PaymentId id;
        final OrderId order;
        final String customer;
        final Money amount;
        EventType status;
        long capturedAtMillis = -1;
        boolean settled;

        Payment(MerchantId merchant, PaymentId id, OrderId order, String customer, Money amount) {
            this.merchant = merchant;
            this.id = id;
            this.order = order;
            this.customer = customer;
            this.amount = amount;
        }

        public EventType status() {
            return status;
        }

        public Money amount() {
            return amount;
        }

        public boolean captured() {
            return capturedAtMillis >= 0;
        }
    }

    /** One per-payment line of a settlement, as the bank would report it. */
    public record SettlementLine(LocalDate settlementDate, MerchantId merchant, PaymentId reference, Money gross,
                                 Money fee, Money gst, Money net) {
    }

    private final Scheduler scheduler;
    private final SimNetwork network;
    private final FakeBank bank;
    private final Ingress ingress;
    private final Trace trace;
    private final TreeMap<MerchantId, byte[]> secrets;
    private final TreeMap<MerchantId, FakeMerchant> merchants;
    private final TreeMap<PaymentId, Payment> payments = new TreeMap<>();
    private final List<Delivery> deliveries = new ArrayList<>();
    private final List<SettlementLine> settlementLines = new ArrayList<>();

    public FakeGateway(Scheduler scheduler, SimNetwork network, FakeBank bank, Ingress ingress, Trace trace,
                       TreeMap<MerchantId, byte[]> secrets, TreeMap<MerchantId, FakeMerchant> merchants) {
        this.scheduler = scheduler;
        this.network = network;
        this.bank = bank;
        this.ingress = ingress;
        this.trace = trace;
        this.secrets = secrets;
        this.merchants = merchants;
        bank.open(FLOAT, 0);
        bank.open(REVENUE, 0);
    }

    /** The customer starts paying: the payment exists, PENDING, and the gateway says so. */
    public Payment start(MerchantId merchant, PaymentId id, OrderId order, String customer, Money amount) {
        Payment p = new Payment(merchant, id, order, customer, amount);
        payments.put(id, p);
        emit(p, EventType.PAYMENT_PENDING);
        return p;
    }

    /** The bank confirms the debit: money moves into the float, the gateway reports it, and maybe tells the merchant. */
    public void succeed(Payment p, boolean merchantHears) {
        bank.transfer(p.customer, FLOAT, p.amount.paise());
        p.capturedAtMillis = scheduler.now();
        emit(p, EventType.PAYMENT_SUCCEEDED);
        if (merchantHears) {
            FakeMerchant merchant = merchants.get(p.merchant);
            network.send(merchant.merchantId().value(), () -> merchant.onPaymentResult(p.order, p.id, true));
        }
    }

    public void fail(Payment p) {
        emit(p, EventType.PAYMENT_FAILED);
    }

    /** The status API (DESIGN §9.1): the current status, as the gateway knows it. Staleness arrives in Shift 8. */
    public Optional<EventType> status(PaymentId id) {
        return Optional.ofNullable(payments.get(id)).map(Payment::status);
    }

    /** Settles every capture whose IST date is before {@code settlementDate}: net to the merchant, fees to revenue. */
    public void settle(LocalDate settlementDate) {
        for (Payment p : payments.values()) {
            if (!p.captured() || p.settled) {
                continue;
            }
            LocalDate captured = IstCalendar.istDate(scheduler.epoch().plusMillis(p.capturedAtMillis));
            if (!captured.isBefore(settlementDate)) {
                continue;
            }
            long gross = p.amount.paise();
            long fee = roundHalfUp(Math.multiplyExact(gross, FEE_BPS));
            long gst = roundHalfUp(Math.multiplyExact(fee, GST_BPS));
            long net = gross - fee - gst;
            bank.transfer(FLOAT, merchantAccount(p.merchant), net);
            if (fee + gst > 0) {
                bank.transfer(FLOAT, REVENUE, fee + gst);
            }
            p.settled = true;
            settlementLines.add(new SettlementLine(settlementDate, p.merchant, p.id, p.amount, Money.ofPaise(fee),
                    Money.ofPaise(gst), Money.ofPaise(net)));
        }
        trace.line(scheduler.now(), "gateway", "settles " + settlementDate + " lines=" + settlementLines.size());
    }

    public static String merchantAccount(MerchantId merchant) {
        return "merchant:" + merchant.value();
    }

    private static long roundHalfUp(long basisPointProduct) {
        return (basisPointProduct + 5_000) / 10_000;
    }

    private void emit(Payment p, EventType type) {
        p.status = type;
        String eventId = "evt_" + DerivedId.of("gateway-event", p.merchant.value(), p.id.value(), type.wireName()).value();
        byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook(eventId, type, scheduler.instant(),
                new PaymentPayload(p.id, p.order, p.amount)));
        byte[] secret = secrets.get(p.merchant);
        trace.line(scheduler.now(), "gateway", type.wireName() + " " + p.id + " " + p.amount);
        Delivery delivery = new Delivery(scheduler, "gateway", (attemptNo, reply) -> {
            String signature = WebhookSignature.sign(secret, scheduler.instant(), body);
            network.send("ingest", () -> ingress.receive(new Request.Webhook(p.merchant, body, Optional.of(signature)),
                    status -> network.send("gateway", () -> reply.accept(status))));
        }, at -> null);
        deliveries.add(delivery);
        delivery.start();
    }

    public List<SettlementLine> settlementLines() {
        return List.copyOf(settlementLines);
    }

    public TreeMap<PaymentId, Payment> payments() {
        return new TreeMap<>(payments);
    }

    /** Deliveries never acknowledged, including those given up on. */
    public long undelivered() {
        return deliveries.stream().filter(d -> !d.acked()).count();
    }
}
