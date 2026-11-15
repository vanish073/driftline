package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The payment gateway (DESIGN §11.4): it holds payments and their status, moves the customer's money into its float on
 * success, sends signed webhooks on a retry schedule (each attempt re-signed with a fresh timestamp), answers status
 * queries, and settles each day's captures to the merchant net of fees and GST: the money moves, and one settlement
 * file per merchant per date goes to Driftline through the {@link SettlementFeed}, even on a day with nothing to
 * settle (docs/formats.md). The settlement scenarios play out here: a payment's line can be short (with or without
 * itemisation that explains the gap), withheld (never settled), or followed by an extra line with an unknown
 * reference. Every one of them moves real money, so INV-WORLD-CONSERVED still holds.
 */
public final class FakeGateway {

    public static final String FLOAT = "gateway:float";
    public static final String REVENUE = "gateway:revenue";
    /** Where the money for extra (orphan) lines comes from. */
    public static final String RESERVE = "gateway:reserve";
    static final long RESERVE_PAISE = 1_000_000_000L;
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
        boolean failedBeforeCapture;
        boolean settled;
        SettlementPlan plan = SettlementPlan.NORMAL;
        LocalDate settledOn;
        String lineKey;

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

        public MerchantId merchant() {
            return merchant;
        }

        public PaymentId id() {
            return id;
        }

        public OrderId order() {
            return order;
        }

        public long capturedAtMillis() {
            return capturedAtMillis;
        }

        /** The gateway reported a failure before it captured the payment: a LATE_SUCCESS in the world. */
        public boolean failedBeforeCapture() {
            return failedBeforeCapture;
        }

        /** The settlement date of the file that settled it, if any. */
        public Optional<LocalDate> settledOn() {
            return Optional.ofNullable(settledOn);
        }

        public Optional<String> lineKey() {
            return Optional.ofNullable(lineKey);
        }

        public SettlementPlan plan() {
            return plan;
        }
    }

    /** How the bank settles one payment (the settlement scenarios of §11.4). */
    public enum SettlementPlan {
        NORMAL,
        /** Paid short: half the time with itemisation that explains the gap exactly. */
        SHORT,
        /** Never settled: a MISSING_SETTLEMENT in the world. */
        WITHHELD,
        /** Settled normally, and followed by an extra line with an unknown reference. */
        WITH_ORPHAN
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
    private final SplitMix64 rng;
    private final SettlementFeed feed;
    private final TreeSet<MerchantId> itemising = new TreeSet<>();
    private final TreeSet<String> orphanLines = new TreeSet<>();
    private final TreeSet<String> shortLines = new TreeSet<>();

    /**
     * @param rng the {@code bank} stream: which merchants itemise fees, and the amounts of short and extra lines
     */
    public FakeGateway(Scheduler scheduler, SimNetwork network, FakeBank bank, Ingress ingress, Trace trace,
                       TreeMap<MerchantId, byte[]> secrets, TreeMap<MerchantId, FakeMerchant> merchants, SplitMix64 rng,
                       SettlementFeed feed) {
        this.scheduler = scheduler;
        this.network = network;
        this.bank = bank;
        this.ingress = ingress;
        this.trace = trace;
        this.secrets = secrets;
        this.merchants = merchants;
        this.rng = rng;
        this.feed = feed;
        bank.open(FLOAT, 0);
        bank.open(REVENUE, 0);
        bank.open(RESERVE, RESERVE_PAISE);
        for (MerchantId m : merchants.keySet()) {
            if (rng.chance(1, 2)) {
                itemising.add(m);
            }
        }
    }

    /** Decides how the bank will settle {@code p} (the workload's settlement scenarios). */
    public void plan(Payment p, SettlementPlan plan) {
        p.plan = plan;
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
        p.failedBeforeCapture = p.status == EventType.PAYMENT_FAILED;
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

    /**
     * Settles every capture whose IST date is before {@code settlementDate} (a T+1 cycle) and sends each merchant its
     * file for that date: net to the merchant, fees and GST to revenue, one line per payment.
     */
    public void settle(LocalDate settlementDate) {
        String day = settlementDate.toString().replace("-", "");
        for (MerchantId m : merchants.keySet()) {
            String fileId = "stl_" + m.value() + "_" + day;
            String batch = "UTR" + day + m.value().replace("_", "");
            boolean itemises = itemising.contains(m);
            List<SettlementLinePayload> lines = new ArrayList<>();
            for (Payment p : payments.values()) {
                if (!p.merchant.equals(m) || !p.captured() || p.settled || p.plan == SettlementPlan.WITHHELD) {
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
                long received = net;
                Optional<Money> lineFee = itemises ? Optional.of(Money.ofPaise(fee)) : Optional.empty();
                Optional<Money> lineTax = itemises ? Optional.of(Money.ofPaise(gst)) : Optional.empty();
                if (p.plan == SettlementPlan.SHORT) {
                    long shortBy = rng.between(100L, Math.max(100L, Math.min(5_000L, net - 1)));
                    long mode = rng.nextLong(4);
                    received = net - shortBy;
                    if (mode < 2) {          // itemised, and the itemised fee explains the gap exactly
                        lineFee = Optional.of(Money.ofPaise(fee + shortBy));
                        lineTax = Optional.of(Money.ofPaise(gst));
                    } else if (mode == 2) {  // not itemised
                        lineFee = Optional.empty();
                        lineTax = Optional.empty();
                    } else {                 // itemised at the contracted rates, which do not explain the gap
                        lineFee = Optional.of(Money.ofPaise(fee));
                        lineTax = Optional.of(Money.ofPaise(gst));
                    }
                }
                bank.transfer(FLOAT, merchantAccount(m), received);
                bank.transfer(FLOAT, REVENUE, gross - received);
                int lineNo = lines.size() + 1;
                lines.add(new SettlementLinePayload(fileId, lineNo, settlementDate, settlementDate,
                        Money.ofPaise(received), lineFee, lineTax, Optional.of(p.id.value()), Optional.of(batch),
                        "settlement " + p.id.value()));
                p.settled = true;
                p.settledOn = settlementDate;
                p.lineKey = fileId + ":" + lineNo;
                if (p.plan == SettlementPlan.SHORT) {
                    shortLines.add(m + "/" + p.lineKey);
                }
                settlementLines.add(new SettlementLine(settlementDate, m, p.id, p.amount, Money.ofPaise(fee),
                        Money.ofPaise(gst), Money.ofPaise(received)));
                if (p.plan == SettlementPlan.WITH_ORPHAN) {
                    long extra = rng.between(100L, 50_000L);
                    bank.transfer(RESERVE, merchantAccount(m), extra);
                    int orphanNo = lines.size() + 1;
                    lines.add(new SettlementLinePayload(fileId, orphanNo, settlementDate, settlementDate,
                            Money.ofPaise(extra), Optional.empty(), Optional.empty(),
                            Optional.of("ext_" + p.id.value()), Optional.of(batch), "unidentified credit"));
                    orphanLines.add(m + "/" + fileId + ":" + orphanNo);
                }
            }
            feed.deliver(SettlementFile.of(fileId, m, settlementDate, scheduler.instant(), lines));
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

    /** {@code merchant/fileId:lineNo} of every extra line with an unknown reference. */
    public TreeSet<String> orphanLines() {
        return new TreeSet<>(orphanLines);
    }

    /** {@code merchant/fileId:lineNo} of every short line. */
    public TreeSet<String> shortLines() {
        return new TreeSet<>(shortLines);
    }

    public Optional<Payment> payment(PaymentId id) {
        return Optional.ofNullable(payments.get(id));
    }

    public TreeMap<PaymentId, Payment> payments() {
        return new TreeMap<>(payments);
    }

    /** Deliveries never acknowledged, including those given up on. */
    public long undelivered() {
        return deliveries.stream().filter(d -> !d.acked()).count();
    }
}
