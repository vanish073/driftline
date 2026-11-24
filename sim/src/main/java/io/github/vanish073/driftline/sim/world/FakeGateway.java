package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
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
import java.util.function.Consumer;

/**
 * The payment gateway (DESIGN §11.4): it holds payments and their status, moves the customer's money into its float on
 * success, sends signed webhooks on a retry schedule (each attempt re-signed with a fresh timestamp), answers status
 * queries, and settles each day's captures to the merchant net of fees and GST: the money moves, and one settlement
 * file per merchant per date goes to Driftline through the {@link SettlementFeed}, even on a day with nothing to
 * settle (docs/formats.md). The settlement scenarios play out here: a payment's line can be short (with or without
 * itemisation that explains the gap), withheld (never settled), or followed by an extra line with an unknown
 * reference. Every one of them moves real money, so INV-WORLD-CONSERVED still holds.
 *
 * <p>Shift 7: on a <em>batch day</em> (about 30% of a merchant's settlement dates) its NORMAL payments are paid in
 * unreferenced batch lines instead, one or two per file, each the exact sum of its payments' nets: the lump-sum
 * credits of §6.4. The gateway remembers which payments make up each batch line, which is the ground truth
 * INV-INFERRED-HONEST checks inferred matches against.
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
        /** Whether a {@code payment.failed} webhook was sent at all (not given up on under WEBHOOK_LOST). */
        boolean failureReported;
        /** Whether a {@code payment.succeeded} webhook was sent at all (not given up on under WEBHOOK_LOST). */
        boolean successReported;
        /** Status-API answers that disagreed with the payment's status at that moment (STATUS_STALE). */
        int staleAnswers;
        boolean settled;
        SettlementPlan plan = SettlementPlan.NORMAL;
        LocalDate settledOn;
        String lineKey;
        /** Every status the gateway gave the payment, with its time: what a stale status API reads (STATUS_STALE). */
        final List<StatusAt> history = new ArrayList<>();
        /** Every refund the gateway executed for the payment (INV-REFUND-ONCE wants at most one). */
        final List<RefundRecord> refunds = new ArrayList<>();

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

        /**
         * Whether the gateway ever reported this payment's failure: false if its only {@code payment.failed} webhook
         * was lost. The status and order APIs answer the current status only, so such a failure is invisible.
         */
        public boolean failureReported() {
            return failureReported;
        }

        /** Whether the gateway ever sent this payment's success webhook (false if it was lost). */
        public boolean successReported() {
            return successReported;
        }

        /** How many status-API answers about this payment disagreed with its status when they were given. */
        public int staleAnswers() {
            return staleAnswers;
        }

        /** The settlement date of the file that settled it, if any. */
        public Optional<LocalDate> settledOn() {
            return Optional.ofNullable(settledOn);
        }

        public Optional<String> lineKey() {
            return Optional.ofNullable(lineKey);
        }

        public List<RefundRecord> refunds() {
            return List.copyOf(refunds);
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

    /** A status the gateway gave a payment, at a scheduler time. */
    public record StatusAt(long atMillis, EventType status) {
    }

    /**
     * A refund the gateway executed: funded from the float when the payment was still unsettled (the gateway then
     * never settles it), from the reserve when it was already settled (the merchant then owes the gateway).
     */
    public record RefundRecord(String refundId, long atMillis, boolean fromReserve, Optional<String> idempotencyKey) {
    }

    /**
     * How long the gateway honours a refund idempotency key. Short in the simulator (production config is 24 h) so the
     * modelled outages, at most 10 minutes, can outlast a key, and the dispatcher's TTL-aware lookup is exercised
     * (D62). The dispatcher is configured with the same value.
     */
    public static final long KEY_TTL_MILLIS = 5 * 60_000L;
    /** The refund lookup API is eventually consistent: a refund shows up there this long after it was executed. */
    public static final long LOOKUP_LAG_MILLIS = 20_000L;

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
    private final FaultInjector faults;
    /** Refund idempotency keys the gateway still remembers: key → (when, the answer it gave). */
    private final TreeMap<String, KeyEntry> refundKeys = new TreeMap<>();
    /** Every refund executed, in order, as {@code paymentId} (for the incremental INV-REFUND-ONCE check). */
    private final List<PaymentId> refundLog = new ArrayList<>();
    /** Every settlement file the gateway produced, by file id: what the world paid in each. */
    private final TreeMap<String, SettlementFile> files = new TreeMap<>();
    private long lostWebhooks;

    private record KeyEntry(long atMillis, TreeMap<String, String> answer) {
    }
    private final TreeSet<String> orphanLines = new TreeSet<>();
    private final TreeSet<String> shortLines = new TreeSet<>();
    /** {@code merchant/fileId:lineNo} of each unreferenced batch line → the payments it pays. */
    private final TreeMap<String, TreeSet<String>> batchLines = new TreeMap<>();
    /** The share of a merchant's settlement dates that are batch days in the simulator. */
    public static final int BATCH_DAY_PERCENT = 30;
    private final int batchDayPercent;
    /** Sandbox only: always two lines on a batch day ({@link #splitBatchesInTwo}). */
    private boolean twoBatchLines;

    /**
     * @param rng the {@code bank} stream: which merchants itemise fees, and the amounts of short and extra lines
     */
    /**
     * @param batchDayPercent the share of settlement dates paid in unreferenced batch lines: {@link #BATCH_DAY_PERCENT}
     *                        in the simulator; 0 in the sandbox, whose Compose suite proves one-to-one detection (M3)
     *                        with exact break sets, while batch matching is proven by the case corpus and the simulator
     */
    public FakeGateway(Scheduler scheduler, SimNetwork network, FakeBank bank, Ingress ingress, Trace trace,
                       TreeMap<MerchantId, byte[]> secrets, TreeMap<MerchantId, FakeMerchant> merchants, SplitMix64 rng,
                       SettlementFeed feed, int batchDayPercent, FaultInjector faults) {
        if (batchDayPercent < 0 || batchDayPercent > 100) {
            throw new IllegalArgumentException("batchDayPercent must be 0 to 100");
        }
        this.batchDayPercent = batchDayPercent;
        this.faults = faults;
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

    /** The current status of a payment, as the gateway knows it (for the world's own checks). */
    public Optional<EventType> status(PaymentId id) {
        return Optional.ofNullable(payments.get(id)).map(Payment::status);
    }

    // ---- the API Driftline's dispatcher calls (DESIGN §9.1, §11.4) ----

    /**
     * Answers one call. {@code reply} receives the facts; it is not called when CALL_AMBIGUOUS loses the response of a
     * refund the gateway did apply.
     */
    public void handle(OutboundCall call, Consumer<TreeMap<String, String>> reply) {
        switch (call.request()) {
            case CallRequest.StatusQuery q -> {
                Payment p = payments.get(q.paymentId());
                TreeMap<String, String> answer = statusAnswer(p, scheduler.now() - faults.statusLagMillis());
                if (p != null && !answer.equals(statusAnswer(p, scheduler.now()))) {
                    p.staleAnswers++;
                }
                reply.accept(answer);
            }
            case CallRequest.StatusQueryOrder q -> reply.accept(orderAnswer(call.merchantId(), q.orderId(),
                    scheduler.now() - faults.statusLagMillis()));
            case CallRequest.Refund r -> {
                TreeMap<String, String> answer = refund(r, call.idempotencyKey());
                if (!faults.callAmbiguous()) {
                    reply.accept(answer);
                }
            }
            case CallRequest.RefundLookup l -> reply.accept(lookup(payments.get(l.paymentId())));
            case CallRequest.PaymentSweep w -> reply.accept(sweepAnswer(call.merchantId(), w.capturedOn(),
                    scheduler.now() - faults.statusLagMillis()));
            default -> throw new IllegalArgumentException("the gateway does not answer " + call.request());
        }
    }

    /** What the status API says about a payment as of {@code asOf}: SUCCEEDED, FAILED, PENDING or UNKNOWN. */
    private TreeMap<String, String> statusAnswer(Payment p, long asOf) {
        TreeMap<String, String> out = new TreeMap<>();
        Optional<StatusAt> seen = p == null ? Optional.empty()
                : p.history.stream().filter(h -> h.atMillis() <= asOf).reduce((a, b) -> b);
        if (seen.isEmpty()) {
            out.put("status", "UNKNOWN");
            return out;
        }
        out.put("status", switch (seen.get().status()) {
            case PAYMENT_SUCCEEDED -> "SUCCEEDED";
            case PAYMENT_FAILED -> "FAILED";
            default -> "PENDING";
        });
        out.put("order_id", p.order.value());
        out.put("amount", p.amount.toString());
        out.put("at", scheduler.epoch().plusMillis(seen.get().atMillis()).toString());
        return out;
    }

    private TreeMap<String, String> orderAnswer(MerchantId m, OrderId order, long asOf) {
        TreeMap<String, String> out = new TreeMap<>();
        int i = 0;
        for (Payment p : payments.values()) {
            if (!p.merchant.equals(m) || !p.order.equals(order)) {
                continue;
            }
            TreeMap<String, String> one = statusAnswer(p, asOf);
            if (one.get("status").equals("UNKNOWN")) {
                continue;
            }
            i++;
            out.put("payment_" + i, p.id.value());
            out.put("status_" + i, one.get("status"));
            out.put("amount_" + i, one.get("amount"));
            out.put("at_" + i, one.get("at"));
        }
        out.put("count", Integer.toString(i));
        return out;
    }

    /**
     * A refund. A key the gateway still remembers (within {@link #KEY_TTL_MILLIS}) gets the answer it got before; a new
     * or forgotten key, or no key, executes a refund, deliberately with no check that the payment was refunded
     * already: a gateway that forgets keys can refund twice (DESIGN §2.2), which is what INV-REFUND-ONCE watches.
     */
    private TreeMap<String, String> refund(CallRequest.Refund r, Optional<String> key) {
        refundKeys.values().removeIf(e -> scheduler.now() - e.atMillis() > KEY_TTL_MILLIS);
        if (key.isPresent() && refundKeys.containsKey(key.get())) {
            faults.count("gateway.keys.replayed");   // a retry deduplicated by its key
            return new TreeMap<>(refundKeys.get(key.get()).answer());
        }
        Payment p = payments.get(r.paymentId());
        TreeMap<String, String> out = new TreeMap<>();
        if (p == null || !p.captured() || !p.amount.equals(r.amount())) {
            out.put("status", "FAILED");
        } else {
            boolean fromReserve = p.settled;
            bank.transfer(fromReserve ? RESERVE : FLOAT, p.customer, p.amount.paise());
            String refundId = "rfd_" + DerivedId.of("gateway-refund", p.id.value(),
                    Integer.toString(p.refunds.size() + 1)).value();
            RefundRecord record = new RefundRecord(refundId, scheduler.now(), fromReserve, key);
            p.refunds.add(record);
            refundLog.add(p.id);
            trace.line(scheduler.now(), "gateway", "refunds " + p.id + " " + p.amount + " as " + refundId
                    + (fromReserve ? " (from the reserve)" : "") + key.map(k -> " key " + k).orElse(" with no key"));
            out.put("status", "PROCESSED");
            out.put("refund_id", refundId);
            out.put("processed_at", scheduler.instant().toString());
            emitRefund(p, record);
        }
        key.ifPresent(k -> refundKeys.put(k, new KeyEntry(scheduler.now(), new TreeMap<>(out))));
        return out;
    }

    /**
     * The captures API (D72): every payment of the merchant captured on that IST day, as the gateway knew them at
     * {@code asOf} (STATUS_STALE makes that earlier than now), with {@code as_of} so the caller can tell a stale view.
     */
    private TreeMap<String, String> sweepAnswer(MerchantId m, LocalDate day, long asOf) {
        TreeMap<String, String> out = new TreeMap<>();
        out.put("as_of", scheduler.epoch().plusMillis(asOf).toString());
        int i = 0;
        for (Payment p : payments.values()) {
            if (!p.merchant.equals(m) || p.capturedAtMillis < 0 || p.capturedAtMillis > asOf
                    || !IstCalendar.istDate(scheduler.epoch().plusMillis(p.capturedAtMillis)).equals(day)) {
                continue;
            }
            i++;
            out.put("payment_" + i, p.id.value());
            out.put("order_" + i, p.order.value());
            out.put("amount_" + i, p.amount.toString());
            out.put("at_" + i, scheduler.epoch().plusMillis(p.capturedAtMillis).toString());
        }
        out.put("count", Integer.toString(i));
        return out;
    }

    /** The refund lookup API: refunds older than {@link #LOOKUP_LAG_MILLIS} (eventually consistent). */
    private TreeMap<String, String> lookup(Payment p) {
        TreeMap<String, String> out = new TreeMap<>();
        Optional<RefundRecord> visible = p == null ? Optional.empty() : p.refunds.stream()
                .filter(r -> scheduler.now() - r.atMillis() >= LOOKUP_LAG_MILLIS).findFirst();
        out.put("found", visible.isPresent() ? "yes" : "no");
        visible.ifPresent(r -> {
            out.put("refund_id", r.refundId());
            out.put("processed_at", scheduler.epoch().plusMillis(r.atMillis()).toString());
        });
        return out;
    }

    /** Every refund executed so far, in order, by payment id. */
    public List<PaymentId> refundLog() {
        return List.copyOf(refundLog);
    }

    /** The settlement file the gateway produced with this id, if any: what the world paid in it. */
    public Optional<SettlementFile> file(String fileId) {
        return Optional.ofNullable(files.get(fileId));
    }

    /** Webhooks the gateway gave up on (WEBHOOK_LOST): never delivered, so not counted as undelivered either. */
    public long lostWebhooks() {
        return lostWebhooks;
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
            boolean batchDay = batchDayPercent > 0 && rng.nextLong(100) < batchDayPercent;
            List<SettlementLinePayload> lines = new ArrayList<>();
            List<Payment> batched = new ArrayList<>();
            for (Payment p : payments.values()) {
                if (!p.merchant.equals(m) || !p.captured() || p.settled || p.plan == SettlementPlan.WITHHELD
                        || !p.refunds.isEmpty()) {
                    continue;
                }
                LocalDate captured = IstCalendar.istDate(scheduler.epoch().plusMillis(p.capturedAtMillis));
                if (!captured.isBefore(settlementDate)) {
                    continue;
                }
                if (batchDay && p.plan == SettlementPlan.NORMAL) {
                    batched.add(p);
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
            if (!batched.isEmpty()) {
                payInBatches(m, settlementDate, fileId, batch, batched, lines);
            }
            SettlementFile file = SettlementFile.of(fileId, m, settlementDate, scheduler.instant(), lines);
            files.put(fileId, file);
            feed.deliver(file);
        }
        trace.line(scheduler.now(), "gateway", "settles " + settlementDate + " lines=" + settlementLines.size());
    }

    /**
     * Pays {@code batched} in one unreferenced line, or in two when there are at least two payments and a coin says so
     * (each payment then goes to either line, neither left empty). A line is the exact sum of its payments' nets, with
     * no fee or tax itemised.
     */
    private void payInBatches(MerchantId m, LocalDate settlementDate, String fileId, String batch, List<Payment> batched,
                              List<SettlementLinePayload> lines) {
        List<List<Payment>> groups = new ArrayList<>();
        groups.add(new ArrayList<>());
        if (batched.size() >= 2 && (twoBatchLines || rng.nextLong(2) == 0)) {
            groups.add(new ArrayList<>());
            for (Payment p : batched) {
                groups.get((int) rng.nextLong(2)).add(p);
            }
            if (groups.get(0).isEmpty() || groups.get(1).isEmpty()) {   // keep both lines non-empty
                List<Payment> all = new ArrayList<>(batched);
                groups.get(0).clear();
                groups.get(1).clear();
                groups.get(0).add(all.get(0));
                groups.get(1).addAll(all.subList(1, all.size()));
            }
        } else {
            groups.get(0).addAll(batched);
        }
        for (List<Payment> group : groups) {
            int lineNo = lines.size() + 1;
            long total = 0;
            TreeSet<String> members = new TreeSet<>();
            for (Payment p : group) {
                long gross = p.amount.paise();
                long fee = roundHalfUp(Math.multiplyExact(gross, FEE_BPS));
                long gst = roundHalfUp(Math.multiplyExact(fee, GST_BPS));
                long net = gross - fee - gst;
                bank.transfer(FLOAT, merchantAccount(m), net);
                bank.transfer(FLOAT, REVENUE, gross - net);
                total = Math.addExact(total, net);
                p.settled = true;
                p.settledOn = settlementDate;
                p.lineKey = fileId + ":" + lineNo;
                members.add(p.id.value());
                settlementLines.add(new SettlementLine(settlementDate, m, p.id, p.amount, Money.ofPaise(fee),
                        Money.ofPaise(gst), Money.ofPaise(net)));
            }
            lines.add(new SettlementLinePayload(fileId, lineNo, settlementDate, settlementDate, Money.ofPaise(total),
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(batch), "batch payout"));
            batchLines.put(m + "/" + fileId + ":" + lineNo, members);
        }
    }

    /**
     * The sandbox's ambiguous-line injection (D68): every batch day is paid in two lines, never one. With three or more
     * equal amounts, each line then fits several subsets of the day's payments, so neither has a unique solution. The
     * simulator never calls this, so its seeds are unchanged.
     */
    public void splitBatchesInTwo() {
        this.twoBatchLines = true;
    }

    /** {@code merchant/fileId:lineNo} of each unreferenced batch line → the payment ids it pays (ground truth). */
    public TreeMap<String, TreeSet<String>> batchLines() {
        TreeMap<String, TreeSet<String>> out = new TreeMap<>();
        batchLines.forEach((k, v) -> out.put(k, new TreeSet<>(v)));
        return out;
    }

    /** The expected net of a payment of {@code gross} under the contracted fee model (2% fee, 18% GST on the fee). */
    public static Money netOf(Money gross) {
        long fee = roundHalfUp(Math.multiplyExact(gross.paise(), FEE_BPS));
        long gst = roundHalfUp(Math.multiplyExact(fee, GST_BPS));
        return Money.ofPaise(gross.paise() - fee - gst);
    }

    public static String merchantAccount(MerchantId merchant) {
        return "merchant:" + merchant.value();
    }

    private static long roundHalfUp(long basisPointProduct) {
        return (basisPointProduct + 5_000) / 10_000;
    }

    private void emit(Payment p, EventType type) {
        p.status = type;
        p.history.add(new StatusAt(scheduler.now(), type));
        String eventId = "evt_" + DerivedId.of("gateway-event", p.merchant.value(), p.id.value(), type.wireName()).value();
        boolean sent = send(p, eventId, type, new PaymentPayload(p.id, p.order, p.amount),
                type.wireName() + " " + p.id + " " + p.amount);
        if (sent && type == EventType.PAYMENT_FAILED) {
            p.failureReported = true;
        }
        if (sent && type == EventType.PAYMENT_SUCCEEDED) {
            p.successReported = true;
        }
    }

    private void emitRefund(Payment p, RefundRecord r) {
        String eventId = "evt_" + DerivedId.of("gateway-refund", p.merchant.value(), r.refundId()).value();
        send(p, eventId, EventType.REFUND_PROCESSED, new RefundPayload(p.id, p.amount),
                "refund.processed " + p.id + " " + r.refundId());
    }

    /**
     * A signed webhook with retries; under WEBHOOK_LOST the gateway gives up on it and never delivers it.
     *
     * @return whether the webhook was sent (it is then retried until delivered)
     */
    private boolean send(Payment p, String eventId, EventType type, EventPayload payload, String what) {
        byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook(eventId, type, scheduler.instant(), payload));
        byte[] secret = secrets.get(p.merchant);
        if (faults.webhookLost()) {
            lostWebhooks++;
            trace.line(scheduler.now(), "gateway", what + " (webhook given up on)");
            return false;
        }
        trace.line(scheduler.now(), "gateway", what);
        Delivery delivery = new Delivery(scheduler, "gateway", (attemptNo, answered, reply) -> {
            String signature = WebhookSignature.sign(secret, scheduler.instant(), body);
            network.send("ingest", () -> ingress.receive(new Request.Webhook(p.merchant, body, Optional.of(signature)),
                    status -> {
                        answered.accept(status);
                        network.send("gateway", () -> reply.accept(status));
                    }));
        }, at -> null);
        deliveries.add(delivery);
        delivery.start();
        return true;
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

    /** Deliveries Driftline never accepted though first sent before {@code judgedBefore} (D87), given up on or not. */
    public long undelivered(long judgedBefore) {
        return deliveries.stream().filter(d -> d.undrained(judgedBefore)).count();
    }
}
