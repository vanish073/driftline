package io.github.vanish073.driftline.sim.invariant;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakKind;
import io.github.vanish073.driftline.core.breaks.BreakLifecycle;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.CauseType;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.matcher.MatcherConfig;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo.Line;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.settlement.BatchMatching;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.sim.world.FakeBank;
import io.github.vanish073.driftline.sim.world.FakeGateway;
import io.github.vanish073.driftline.sim.world.FakeMerchant;
import io.github.vanish073.driftline.sim.world.SettlementFeed;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * The invariants of DESIGN §11.7 checked so far (Shifts 3 to 7). Incremental checks run after every step and look
 * only at rows committed since the previous step; end-state checks run once the run has drained.
 */
public final class Invariants {

    public static final String LEDGER_BALANCED = "INV-LEDGER-BALANCED";
    public static final String LEDGER_ONCE = "INV-LEDGER-ONCE";
    public static final String EVENT_ONCE = "INV-EVENT-ONCE";
    public static final String TRANSITIONS_LEGAL = "INV-TRANSITIONS-LEGAL";
    public static final String ONE_OUTCOME = "INV-ONE-OUTCOME";
    public static final String WORLD_CONSERVED = "INV-WORLD-CONSERVED";
    /** No offset is ever committed under a partition epoch that has been superseded (§5.1). */
    public static final String FENCED = "INV-FENCED";
    /**
     * Matcher anomalies that are always bugs: a ledger conflict, or a quarantined key that the world never reused
     * (so Driftline split one event in two).
     */
    public static final String NO_ANOMALY = "INV-NO-ANOMALY";
    /** End state: every record appended to the log was processed, and every world message was acknowledged. */
    public static final String DRAINED = "INV-DRAINED";
    /** A MISSING_SETTLEMENT break exists only if the world did not settle that payment by its due date plus grace. */
    public static final String MISSING_IS_REAL = "INV-MISSING-IS-REAL";
    /** End state: every one-to-one break the world implies (the oracle) has a Driftline break of that type. */
    public static final String DETECTED = "INV-DETECTED";
    /** End state: every MATCHED_INFERRED line pays exactly the payments the world put in it, or is challenged (§6.4). */
    public static final String INFERRED_HONEST = "INV-INFERRED-HONEST";
    /** Every AMBIGUOUS line has two distinct witnesses, each a set of open payments in its window summing to it. */
    public static final String AMBIGUITY_WITNESSED = "INV-AMBIGUITY-WITNESSED";
    /** The gateway executed at most one refund per payment, for its captured amount (Shift 8). */
    public static final String REFUND_ONCE = "INV-REFUND-ONCE";
    /** No payment is both refunded and the payment its fulfilled order was paid with. */
    public static final String NO_REFUND_OF_FULFILLED = "INV-NO-REFUND-OF-FULFILLED";
    /** Every break is closed or escalated by max(escalate_by, detected_at) plus the reaction bound (§8.3, D63). */
    public static final String DEADLINE = "INV-DEADLINE";
    /** End state: the books match the world: BANK per merchant, and every refund booked once (§11.7). */
    public static final String BOOKS_MATCH_WORLD = "INV-BOOKS-MATCH-WORLD";
    /** End state: every debited payment is fulfilled, refunded once, or escalated; a decided refund is never lost. */
    public static final String CUSTOMER_WHOLE = "INV-CUSTOMER-WHOLE";

    /**
     * How long after max(escalate_by, detected_at) a break may still be open: longer than the longest modelled outage
     * of the resolver or the store (10 minutes) plus a review, so an outage at the moment of escalation is no violation.
     */
    static final Duration REACTION_BOUND = Duration.ofHours(1);
    /** The breaks that name a payment, whose escalation covers that payment for INV-CUSTOMER-WHOLE. */
    /** The breaks whose subject is a settlement line ({@code fileId:lineNo}). */
    static final List<BreakType> LINE_BREAKS = List.of(BreakType.ORPHAN_SETTLEMENT, BreakType.SHORT_SETTLEMENT,
            BreakType.SETTLEMENT_CONFLICT, BreakType.OVER_SETTLEMENT);
    static final List<BreakType> ABOUT_A_PAYMENT = List.of(BreakType.DEBITED_NO_ORDER, BreakType.LATE_SUCCESS,
            BreakType.DUPLICATE_CAPTURE, BreakType.STATUS_CONFLICT, BreakType.MISSING_SETTLEMENT);

    /** Unreferenced lines whose money is still in suspense, escalated as ORPHAN_SETTLEMENT (§6.4). */
    static final List<LineStatus> UNRESOLVED_BATCH = List.of(LineStatus.AMBIGUOUS, LineStatus.NO_SOLUTION,
            LineStatus.BUDGET_EXCEEDED, LineStatus.OUT_OF_RANGE);
    static final int LOOKBACK_DAYS = MatcherConfig.DEFAULT.batchLookbackDays();
    /**
     * The breaks found in the stream (R2), each about a payment the world captured: how long after that capture the
     * break was first committed is counted in {@link #LAG_BUCKETS}. Settlement breaks wait for the bank's file or the
     * due date by design (§5.5), so they are not measured.
     */
    static final List<BreakType> STREAM_BREAKS = List.of(BreakType.LATE_SUCCESS, BreakType.DUPLICATE_CAPTURE,
            BreakType.DEBITED_NO_ORDER);
    /** Upper bounds of the detection-lag buckets, in minutes; a lag above the last is counted as {@code gt_1d}. */
    static final List<Long> LAG_BUCKETS = List.of(1L, 5L, 10L, 15L, 30L, 60L, 360L, 1440L);

    /**
     * Anomalies the simulated world never causes, so Driftline raising one is a bug. FILE_CONFLICT is expected only
     * for a merchant the bank sent a conflicting file for; IDEMPOTENCY_CONFLICT only with a quarantined record, which
     * the quarantine check above ties to a key the world really reused; FEED_STALL whenever a file is late.
     * INFERRED_MATCH_CHALLENGED is possible from Shift 7: a success delivered after its batch line was solved.
     * STATUS_CONFLICT is possible from Shift 8, but only when the gateway gave at least {@code maxStatusQueries} stale
     * status answers about the payment (STATUS_STALE), so it is checked on its own.
     */
    static final List<BreakType> NEVER_EXPECTED = List.of(BreakType.ORDER_ANOMALY,
            BreakType.OVER_SETTLEMENT, BreakType.SETTLEMENT_CONFLICT, BreakType.LEDGER_CONFLICT);

    private final InMemoryStore store;
    private final FakeBank bank;
    private final List<String> anomalies = new ArrayList<>();
    private int ledgerSeen;
    private int paymentSeen;
    private int orderSeen;
    private int offsetCommitSeen;
    private int quarantineSeen;
    private final Supplier<Set<String>> reusedKeys;
    private final TreeSet<String> causes = new TreeSet<>();
    private final TreeSet<String> appliedEventKeys = new TreeSet<>();
    private final TreeMap<String, PaymentStatus> paymentStatus = new TreeMap<>();
    private int breakSeen;
    private FakeGateway gateway;
    private SettlementFeed feed;
    private TreeMap<MerchantId, FakeMerchant> merchants;
    private Instant epoch;
    private int cycleDays;
    private int graceDays;
    private LocalDate lastFileDate;
    private TreeSet<String> expected = new TreeSet<>();
    private TreeSet<String> detected = new TreeSet<>();
    private long coveredByIdempotencyConflict;
    private long coveredByEscalatedLine;
    private int breakMoveSeen;
    private int refundSeen;
    private long lastDeadlineCheck = Long.MIN_VALUE;
    private long coveredByUnresolvedBatch;
    private final TreeMap<String, Long> detectionLag = new TreeMap<>();

    /** @param reusedKeys the keys the world's buggy clients reused (MERCHANT_KEY_REUSE), per merchant and key */
    public Invariants(InMemoryStore store, FakeBank bank, Supplier<Set<String>> reusedKeys) {
        this.store = store;
        this.bank = bank;
        this.reusedKeys = reusedKeys;
    }

    /** Connects the world the settlement and detection checks compare against (set once, before the run). */
    public void world(FakeGateway gateway, SettlementFeed feed, TreeMap<MerchantId, FakeMerchant> merchants, Instant epoch,
                      int cycleDays, int graceDays, LocalDate lastFileDate) {
        this.gateway = gateway;
        this.feed = feed;
        this.merchants = merchants;
        this.epoch = epoch;
        this.cycleDays = cycleDays;
        this.graceDays = graceDays;
        this.lastFileDate = lastFileDate;
    }

    public void anomaly(String description) {
        anomalies.add(description);
    }

    public Optional<Violation> afterStep(long now) {
        if (!anomalies.isEmpty()) {
            return Optional.of(new Violation(NO_ANOMALY, anomalies.get(0), now));
        }
        List<LedgerTxn> ledger = store.committedLedger();
        for (; ledgerSeen < ledger.size(); ledgerSeen++) {
            LedgerTxn txn = ledger.get(ledgerSeen);
            Optional<String> unbalanced = Checks.ledgerBalanced(txn);
            if (unbalanced.isPresent()) {
                return Optional.of(new Violation(LEDGER_BALANCED, unbalanced.get(), now));
            }
            if (!causes.add(txn.merchantId() + "/" + txn.cause())) {
                return Optional.of(new Violation(LEDGER_ONCE, "a second transaction for " + txn.cause() + " of "
                        + txn.merchantId(), now));
            }
        }
        List<InMemoryStore.Recorded<PaymentTransition>> payments = store.committedPaymentTransitions();
        for (; paymentSeen < payments.size(); paymentSeen++) {
            InMemoryStore.Recorded<PaymentTransition> r = payments.get(paymentSeen);
            PaymentTransition t = r.row();
            if (!appliedEventKeys.add(r.merchantId() + "/" + t.eventKey())) {
                return Optional.of(new Violation(EVENT_ONCE, "event " + t.eventKey() + " reached a state machine twice", now));
            }
            Optional<String> illegal = Checks.paymentTransitionLegal(t);
            if (illegal.isPresent()) {
                return Optional.of(new Violation(TRANSITIONS_LEGAL, illegal.get(), now));
            }
            String key = r.merchantId() + "/" + t.paymentId();
            Optional<String> twoFinal = Checks.oneFinalState(paymentStatus.getOrDefault(key, PaymentStatus.NONE), t);
            if (twoFinal.isPresent()) {
                return Optional.of(new Violation(ONE_OUTCOME, twoFinal.get(), now));
            }
            paymentStatus.put(key, t.to());
        }
        List<InMemoryStore.Recorded<OrderTransition>> orders = store.committedOrderTransitions();
        for (; orderSeen < orders.size(); orderSeen++) {
            InMemoryStore.Recorded<OrderTransition> r = orders.get(orderSeen);
            if (!appliedEventKeys.add(r.merchantId() + "/" + r.row().eventKey())) {
                return Optional.of(new Violation(EVENT_ONCE, "event " + r.row().eventKey() + " reached a state machine twice", now));
            }
            Optional<String> illegal = Checks.orderTransitionLegal(r.row());
            if (illegal.isPresent()) {
                return Optional.of(new Violation(TRANSITIONS_LEGAL, illegal.get(), now));
            }
        }
        List<InMemoryStore.OffsetCommit> commits = store.committedOffsetCommits();
        for (; offsetCommitSeen < commits.size(); offsetCommitSeen++) {
            InMemoryStore.OffsetCommit c = commits.get(offsetCommitSeen);
            if (c.epochUsed() != c.leaseEpoch()) {
                return Optional.of(new Violation(FENCED, c.txName() + " committed p" + c.partition() + " to offset "
                        + c.nextOffset() + " with epoch " + c.epochUsed() + " while the lease was at epoch "
                        + c.leaseEpoch(), now));
            }
        }
        List<QuarantineRepo.Entry> quarantined = store.committedQuarantine();
        for (; quarantineSeen < quarantined.size(); quarantineSeen++) {
            QuarantineRepo.Entry q = quarantined.get(quarantineSeen);
            if (!reusedKeys.get().contains(q.merchantId() + "/" + q.eventKey())) {
                return Optional.of(new Violation(NO_ANOMALY, "quarantined " + q.source() + " key " + q.eventKey()
                        + " at p" + q.partition() + "@" + q.offset() + ", which the world never reused", now));
            }
        }
        List<Break> raises = store.committedBreakRaises();
        for (; breakSeen < raises.size(); breakSeen++) {
            countLag(raises.get(breakSeen), now);
            Optional<Violation> v = checkRaised(raises.get(breakSeen), now);
            if (v.isPresent()) {
                return v;
            }
        }
        if (bank.total() != bank.opened()) {
            return Optional.of(new Violation(WORLD_CONSERVED, "the world holds " + bank.total() + " paise but "
                    + bank.opened() + " were opened", now));
        }
        List<InMemoryStore.Recorded<BreakTransition>> moves = store.committedBreakTransitions();
        for (; breakMoveSeen < moves.size(); breakMoveSeen++) {
            BreakTransition t = moves.get(breakMoveSeen).row();
            if (!BreakLifecycle.allowed(t.from().orElseThrow(), t.to())) {
                return Optional.of(new Violation(TRANSITIONS_LEGAL, "break " + t.breakId() + " moved "
                        + t.from().get() + " -> " + t.to() + ", which the lifecycle does not allow", now));
            }
        }
        if (gateway != null) {
            List<PaymentId> refunds = gateway.refundLog();
            for (; refundSeen < refunds.size(); refundSeen++) {
                FakeGateway.Payment p = gateway.payment(refunds.get(refundSeen)).orElseThrow();
                if (p.refunds().size() > 1) {
                    return Optional.of(new Violation(REFUND_ONCE, p.merchant() + "/" + p.id() + " was refunded "
                            + p.refunds().size() + " times: " + p.refunds(), now));
                }
                if (fulfilledWith(p)) {
                    return Optional.of(new Violation(NO_REFUND_OF_FULFILLED, p.merchant() + "/" + p.id()
                            + " was refunded, but its order was fulfilled with it", now));
                }
            }
            if (now - lastDeadlineCheck >= 60_000L) {
                lastDeadlineCheck = now;
                Optional<Violation> late = deadlines(now);
                if (late.isPresent()) {
                    return late;
                }
            }
        }
        return Optional.empty();
    }

    /** Whether the world's merchant fulfilled this payment's order with this payment. */
    private boolean fulfilledWith(FakeGateway.Payment p) {
        return merchants.get(p.merchant()).order(p.order())
                .filter(o -> o.status() == EventType.ORDER_FULFILLED && o.paidWith().filter(p.id()::equals).isPresent())
                .isPresent();
    }

    /**
     * Counts a newly raised stream break in its detection-lag bucket, in true time: from the world's capture of the
     * payment it names to its first commit; for a duplicate capture, from the order's second capture if that is later
     * (the payment named may be the first one, when the order was paid with the second).
     */
    private void countLag(Break b, long now) {
        if (gateway == null || !STREAM_BREAKS.contains(b.type())) {
            return;
        }
        Optional<FakeGateway.Payment> p = gateway.payment(PaymentId.of(b.subjectKey()))
                .filter(FakeGateway.Payment::captured);
        String bucket = p.map(x -> lagBucket(now - happenedAt(b.type(), x))).orElse("uncaptured");
        detectionLag.merge("detection_lag." + b.type().name() + "." + bucket, 1L, Long::sum);
    }

    /** When the break happened in the world: the payment's capture, or a duplicate's second capture of the order. */
    private long happenedAt(BreakType type, FakeGateway.Payment p) {
        if (type != BreakType.DUPLICATE_CAPTURE) {
            return p.capturedAtMillis();
        }
        TreeSet<Long> captures = new TreeSet<>();
        for (FakeGateway.Payment q : gateway.payments().values()) {
            if (q.captured() && q.merchant().equals(p.merchant()) && q.order().equals(p.order())) {
                captures.add(q.capturedAtMillis());
            }
        }
        // Two captures at the same millisecond are one entry here, so the second capture is then the first one
        long second = captures.size() >= 2 ? captures.higher(captures.first()) : captures.first();
        return Math.max(p.capturedAtMillis(), second);
    }

    /** {@code le_<n>m} for the first bound in {@link #LAG_BUCKETS} the lag is within, or {@code gt_1d}. */
    static String lagBucket(long lagMillis) {
        for (long minutes : LAG_BUCKETS) {
            if (lagMillis <= minutes * 60_000L) {
                return "le_" + minutes + "m";
            }
        }
        return "gt_1d";
    }

    /** Raised stream breaks by type and detection-lag bucket ({@code detection_lag.TYPE.le_15m} → count). */
    public TreeMap<String, Long> detectionLag() {
        return new TreeMap<>(detectionLag);
    }

    /** INV-DEADLINE: every break still open is within max(escalate_by, detected_at) + the reaction bound. */
    private Optional<Violation> deadlines(long now) {
        Instant at = epoch.plusMillis(now);
        for (Break b : store.committedBreaks()) {
            if (BreakLifecycle.isHandled(b.state())) {
                continue;
            }
            Optional<BreakRepo.Plan> plan = store.committedPlan(b.merchantId(), b.breakId());
            Instant from = plan.map(p -> p.escalateBy().isAfter(b.detectedAt()) ? p.escalateBy() : b.detectedAt())
                    .orElse(b.detectedAt());
            if (at.isAfter(from.plus(REACTION_BOUND))) {
                return Optional.of(new Violation(DEADLINE, b.type() + " " + b.merchantId() + "/" + b.subjectKey()
                        + " is still " + b.state() + (plan.isEmpty() ? ", never reviewed, " : ", ")
                        + "past " + from + " + " + REACTION_BOUND, now));
            }
        }
        return Optional.empty();
    }

    /** {@code merchant/fileId:lineNo} → the breaks on that settlement line. */
    private TreeMap<String, List<Break>> lineBreaks() {
        TreeMap<String, List<Break>> out = new TreeMap<>();
        for (Break b : store.committedBreaks()) {
            if (LINE_BREAKS.contains(b.type())) {
                out.computeIfAbsent(b.merchantId() + "/" + b.subjectKey(), k -> new ArrayList<>()).add(b);
            }
        }
        return out;
    }

    /**
     * An expected break about a payment Driftline could not know succeeded: its success webhook was lost, so it is
     * visible only as money in a settlement line, and the break on that line went to a person.
     */
    private boolean coveredByEscalatedLine(String expectedKey, TreeMap<String, List<Break>> lineBreaks) {
        String subject = expectedKey.substring(expectedKey.indexOf('/') + 1);
        if (subject.indexOf(':') >= 0) {
            return false;   // a line itself
        }
        return gateway.payment(PaymentId.of(subject)).filter(p -> onAnEscalatedLine(p, lineBreaks)).isPresent();
    }

    /**
     * Whether a payment Driftline could not know succeeded (its success webhook was lost) is visible only as money in
     * a settlement line that went to a person, with nothing Driftline could have asked: an unreferenced batch line, or
     * a referenced line whose reference the gateway answered stale every time Driftline asked (D64).
     */
    private boolean onAnEscalatedLine(FakeGateway.Payment p, TreeMap<String, List<Break>> lineBreaks) {
        if (p.successReported() || p.lineKey().isEmpty()) {
            return false;
        }
        String line = p.merchant() + "/" + p.lineKey().get();
        boolean nothingToAsk = gateway.batchLines().containsKey(line)
                || p.staleAnswers() >= Resolver.Config.DEFAULT.maxStatusQueries();
        return nothingToAsk && lineBreaks.getOrDefault(line, List.of()).stream().anyMatch(Invariants::withAPerson);
    }

    /** Expected breaks covered by an escalated break on the line that carries an unreported payment's money. */
    public long coveredByEscalatedLine() {
        return coveredByEscalatedLine;
    }

    private static boolean withAPerson(Break b) {
        return b.state() == BreakState.ESCALATED || b.state() == BreakState.RESOLVED_MANUAL;
    }

    /**
     * INV-CUSTOMER-WHOLE: every payment the world captured is the one its fulfilled order was paid with, or was refunded
     * exactly once, or a break about it was escalated to a person: a break naming the payment, an IDEMPOTENCY_CONFLICT
     * on its order, or, if its success webhook was lost (so Driftline cannot know it succeeded), a break on the
     * settlement line that really carries its money, when Driftline had nothing to ask about it. A payment Driftline decided to refund must have been
     * refunded, whatever else happened: no decided refund is ever lost (§2.1 (d)).
     */
    private Optional<Violation> customersWhole(long now) {
        TreeMap<String, List<Break>> about = new TreeMap<>();
        TreeMap<String, List<Break>> lineBreaks = lineBreaks();
        TreeSet<String> escalatedOrders = new TreeSet<>();
        for (Break b : store.committedBreaks()) {
            if (ABOUT_A_PAYMENT.contains(b.type())) {
                about.computeIfAbsent(b.merchantId() + "/" + b.subjectKey(), k -> new ArrayList<>()).add(b);
            }
            if (b.type() == BreakType.IDEMPOTENCY_CONFLICT && b.evidence().containsKey("order_id")
                    && (b.state() == BreakState.ESCALATED || b.state() == BreakState.RESOLVED_MANUAL)) {
                escalatedOrders.add(b.merchantId() + "/" + b.evidence().get("order_id"));
            }
        }
        for (FakeGateway.Payment p : gateway.payments().values()) {
            if (!p.captured()) {
                continue;
            }
            List<Break> breaks = about.getOrDefault(p.merchant() + "/" + p.id(), List.of());
            boolean decided = breaks.stream().anyMatch(b -> store.committedPlan(b.merchantId(), b.breakId())
                    .flatMap(BreakRepo.Plan::action).filter("REFUND"::equals).isPresent());
            if (decided && p.refunds().isEmpty()) {
                return Optional.of(new Violation(CUSTOMER_WHOLE, "Driftline decided to refund " + p.merchant() + "/"
                        + p.id() + ", but the refund never happened", now));
            }
            if (fulfilledWith(p) || p.refunds().size() == 1) {
                continue;
            }
            boolean escalated = breaks.stream().anyMatch(Invariants::withAPerson)
                    || escalatedOrders.contains(p.merchant() + "/" + p.order())
                    || onAnEscalatedLine(p, lineBreaks);
            if (!escalated) {
                String line = p.lineKey().map(k -> p.merchant() + "/" + k).orElse("none");
                return Optional.of(new Violation(CUSTOMER_WHOLE, p.merchant() + "/" + p.id() + " was debited, but its "
                        + "order was not fulfilled with it, it was not refunded, and no break about it was escalated"
                        + " (success reported " + p.successReported() + ", line " + line
                        + (gateway.batchLines().containsKey(line) ? " (batch)" : "") + ", stale answers "
                        + p.staleAnswers() + ", line breaks " + lineBreaks.getOrDefault(line, List.of()).stream()
                                .map(b -> b.type() + ":" + b.state()).toList()
                        + ", breaks " + breaks.stream().map(b -> b.type() + ":" + b.state()).toList() + ")", now));
            }
        }
        return Optional.empty();
    }

    /**
     * INV-BOOKS-MATCH-WORLD: for each merchant, BANK in the ledger equals what the world paid in the settlement files
     * Driftline completed; and a refund is booked (one REFUND transaction) exactly when the gateway executed one.
     */
    private Optional<Violation> booksMatchWorld(long now) {
        TreeMap<String, Long> expectedBank = new TreeMap<>();
        for (SettlementRepo.FileEnd f : store.committedFileEnds()) {
            if (!f.complete()) {
                continue;
            }
            Optional<SettlementFile> world = gateway.file(f.fileId());
            if (world.isEmpty()) {
                return Optional.of(new Violation(BOOKS_MATCH_WORLD, "Driftline completed file " + f.fileId()
                        + ", which the world never produced", now));
            }
            expectedBank.merge(f.merchantId().value(), world.get().totalAmount().paise(), Math::addExact);
        }
        TreeMap<String, Long> bookedBank = new TreeMap<>();
        TreeSet<String> refundsBooked = new TreeSet<>();
        for (LedgerTxn txn : store.committedLedger()) {
            for (Entry e : txn.entries()) {
                if (e.account() == AccountCode.BANK) {
                    bookedBank.merge(e.merchantId().value(), e.amount().paise(), Math::addExact);
                }
            }
            if (txn.cause().type() == CauseType.REFUND) {
                refundsBooked.add(txn.merchantId() + "/" + txn.cause().id());
            }
        }
        for (String m : merchantsWithBank(expectedBank, bookedBank)) {
            long expected = expectedBank.getOrDefault(m, 0L);
            long booked = bookedBank.getOrDefault(m, 0L);
            if (expected != booked) {
                return Optional.of(new Violation(BOOKS_MATCH_WORLD, m + ": BANK in the ledger is " + booked
                        + " paise, but the world paid " + expected + " paise in the files Driftline completed", now));
            }
        }
        for (FakeGateway.Payment p : gateway.payments().values()) {
            boolean refunded = !p.refunds().isEmpty();
            boolean booked = refundsBooked.contains(p.merchant() + "/" + p.id());
            if (refunded != booked) {
                return Optional.of(new Violation(BOOKS_MATCH_WORLD, p.merchant() + "/" + p.id() + (refunded
                        ? " was refunded by the gateway, but no REFUND is booked"
                        : " has a REFUND booked, but the gateway never refunded it"), now));
            }
        }
        return Optional.empty();
    }

    private static TreeSet<String> merchantsWithBank(TreeMap<String, Long> a, TreeMap<String, Long> b) {
        TreeSet<String> all = new TreeSet<>(a.keySet());
        all.addAll(b.keySet());
        return all;
    }

    private Optional<Violation> checkRaised(Break b, long now) {
        if (b.type() == BreakType.STATUS_CONFLICT) {
            // Possible only if the gateway's status API really contradicted itself every time Driftline asked (D62)
            int stale = gateway == null ? 0 : gateway.payment(PaymentId.of(b.subjectKey()))
                    .map(FakeGateway.Payment::staleAnswers).orElse(0);
            if (stale < Resolver.Config.DEFAULT.maxStatusQueries()) {
                return Optional.of(new Violation(NO_ANOMALY, "STATUS_CONFLICT " + b.merchantId() + "/"
                        + b.subjectKey() + " " + b.evidence() + ", but the gateway gave only " + stale
                        + " stale status answers about it", now));
            }
            return Optional.empty();
        }
        if (NEVER_EXPECTED.contains(b.type())) {
            return Optional.of(new Violation(NO_ANOMALY, b.type() + " " + b.merchantId() + "/" + b.subjectKey()
                    + " " + b.evidence() + ", which the world never causes", now));
        }
        if (b.type() == BreakType.FILE_CONFLICT && !b.evidence().containsKey("file_id")) {
            // Raised by the matcher's own guard, not by the worker at upload: a refused file reached the log.
            return Optional.of(new Violation(NO_ANOMALY, "FILE_CONFLICT " + b.merchantId() + "/" + b.subjectKey()
                    + " raised by the matcher: a file the worker should have refused reached the log", now));
        }
        if (b.type() == BreakType.FILE_CONFLICT && feed != null
                && feed.conflicting().stream().noneMatch(c -> c.startsWith(b.merchantId() + "/"))) {
            return Optional.of(new Violation(NO_ANOMALY, "FILE_CONFLICT " + b.merchantId() + "/" + b.subjectKey()
                    + ", but the bank sent no conflicting file", now));
        }
        if (b.type() == BreakType.INFERRED_MATCH_CHALLENGED && gateway != null) {
            Optional<FakeGateway.Payment> p = Optional.ofNullable(b.evidence().get("payment_id"))
                    .flatMap(id -> gateway.payment(PaymentId.of(id)));
            if (p.isEmpty() || !p.get().captured()) {
                return Optional.of(new Violation(NO_ANOMALY, "INFERRED_MATCH_CHALLENGED " + b.merchantId() + "/"
                        + b.subjectKey() + " names " + b.evidence().get("payment_id")
                        + ", which the world never captured", now));
            }
        }
        if (b.type() == BreakType.ORPHAN_SETTLEMENT && "AMBIGUOUS".equals(b.evidence().get("reason"))) {
            Optional<Violation> v = witnessed(b.merchantId(), b.subjectKey(), now);
            if (v.isPresent()) {
                return v;
            }
        }
        if (b.type() == BreakType.MISSING_SETTLEMENT && gateway != null) {
            Optional<FakeGateway.Payment> p = gateway.payment(PaymentId.of(b.subjectKey()));
            if (p.isEmpty() || !p.get().captured()) {
                return Optional.of(new Violation(MISSING_IS_REAL, "MISSING_SETTLEMENT for " + b.subjectKey()
                        + ", which the world never captured", now));
            }
            LocalDate deadline = Oracle.lastOnTimeDate(epoch.plusMillis(p.get().capturedAtMillis()), cycleDays, graceDays);
            if (p.get().settledOn().filter(d -> !d.isAfter(deadline)).isPresent()) {
                return Optional.of(new Violation(MISSING_IS_REAL, "MISSING_SETTLEMENT for " + b.subjectKey()
                        + ", which the world settled on " + p.get().settledOn().get() + " (in "
                        + p.get().lineKey().orElse("?") + "), by its deadline " + deadline, now));
            }
        }
        return Optional.empty();
    }

    /**
     * A DEBITED_NO_ORDER or DUPLICATE_CAPTURE the world implies for a payment whose order had a merchant event refused
     * as IDEMPOTENCY_CONFLICT. Driftline cannot know that order's true state (the refused event may be the one that
     * says which payment paid it), and it escalated the order instead, which INV-DETECTED accepts ("a break or an
     * escalation", DESIGN §11.7). Found by seed 361 (BUG-001).
     */
    private boolean coveredByEscalation(String expectedKey, TreeSet<String> escalatedOrders) {
        boolean orderDependent = expectedKey.startsWith(BreakType.DEBITED_NO_ORDER + " ")
                || expectedKey.startsWith(BreakType.DUPLICATE_CAPTURE + " ");
        if (!orderDependent) {
            return false;
        }
        String subject = expectedKey.substring(expectedKey.indexOf('/') + 1);
        return gateway.payment(PaymentId.of(subject))
                .filter(p -> escalatedOrders.contains(p.merchant() + "/" + p.order()))
                .isPresent();
    }

    /**
     * INV-AMBIGUITY-WITNESSED for the line {@code fileId:lineNo}: it is AMBIGUOUS with two distinct witnesses, and each
     * witness is a set of distinct payments the world captured, whose batch dates lie in the line's window and whose
     * nets (the contracted fee model) sum to the line's amount. A payment whose outcome Driftline has not learned (its
     * last recorded status is PENDING) counts as possibly captured, inside the window, if Driftline first saw it no
     * later than the window allows (D69, BUG-007): Driftline cannot rule it out, and nothing else could. A witness
     * naming a payment whose outcome Driftline has since learned is stale: the line should have been solved again.
     */
    private Optional<Violation> witnessed(MerchantId m, String lineKey, long now) {
        Optional<Line> found = store.committedLines().stream()
                .filter(l -> l.merchantId().equals(m) && l.key().equals(lineKey)).findFirst();
        if (found.isEmpty() || found.get().status() != LineStatus.AMBIGUOUS) {
            return Optional.of(new Violation(AMBIGUITY_WITNESSED, "ORPHAN_SETTLEMENT " + m + "/" + lineKey
                    + " says AMBIGUOUS, but the line is " + found.map(l -> l.status().name()).orElse("missing"), now));
        }
        Line line = found.get();
        List<List<PaymentId>> w = line.witnesses();
        if (w.size() != 2 || w.get(0).equals(w.get(1))) {
            return Optional.of(new Violation(AMBIGUITY_WITNESSED, m + "/" + lineKey + " has witnesses " + w
                    + ", not two distinct ones", now));
        }
        if (gateway == null) {
            return Optional.empty();
        }
        LocalDate from = line.settlementDate().minusDays(LOOKBACK_DAYS);
        // what Driftline knows of each payment: when it first saw it, and its last recorded status
        TreeMap<String, Instant> firstSeen = new TreeMap<>();
        TreeMap<String, PaymentStatus> lastStatus = new TreeMap<>();
        for (InMemoryStore.Recorded<PaymentTransition> t : store.committedPaymentTransitions()) {
            if (t.merchantId().equals(m)) {
                String key = t.row().paymentId().value();
                firstSeen.merge(key, t.row().eventTime(), (a, b) -> a.isBefore(b) ? a : b);
                lastStatus.put(key, t.row().to());
            }
        }
        for (List<PaymentId> witness : w) {
            long sum = 0;
            TreeSet<String> distinct = new TreeSet<>();
            for (PaymentId id : witness) {
                Optional<FakeGateway.Payment> p = gateway.payment(id);
                // a payment Driftline still sees as PENDING is a possible member (D69), if it was first seen in time
                boolean possible = p.isPresent() && lastStatus.get(id.value()) == PaymentStatus.PENDING
                        && !BatchMatching.batchDate(firstSeen.get(id.value()), cycleDays).isAfter(line.settlementDate());
                if (p.isEmpty() || !p.get().captured() && !possible || !distinct.add(id.value())) {
                    return Optional.of(new Violation(AMBIGUITY_WITNESSED, m + "/" + lineKey + " witness " + witness
                            + " names " + id + ", not a distinct captured (or possibly captured) payment", now));
                }
                LocalDate batchDate = possible ? line.settlementDate()
                        : BatchMatching.batchDate(epoch.plusMillis(p.get().capturedAtMillis()), cycleDays);
                if (batchDate.isBefore(from) || batchDate.isAfter(line.settlementDate())) {
                    return Optional.of(new Violation(AMBIGUITY_WITNESSED, m + "/" + lineKey + " witness " + witness
                            + " names " + id + ", outside the line's window", now));
                }
                sum = Math.addExact(sum, FakeGateway.netOf(p.get().amount()).paise());
            }
            if (sum != line.amount().paise()) {
                return Optional.of(new Violation(AMBIGUITY_WITNESSED, m + "/" + lineKey + " witness " + witness
                        + " sums to " + sum + " paise, not the line's " + line.amount().paise(), now));
            }
        }
        return Optional.empty();
    }

    /** INV-INFERRED-HONEST: every MATCHED_INFERRED line pays exactly its world payments, or carries a challenge. */
    private Optional<Violation> inferredHonest(long now) {
        TreeMap<String, TreeSet<String>> truth = gateway.batchLines();
        TreeMap<String, TreeSet<String>> inferred = new TreeMap<>();
        for (SettlementRepo.Match match : store.committedMatches()) {
            if (match.how() == SettlementRepo.MatchHow.INFERRED) {
                inferred.computeIfAbsent(match.merchantId() + "/" + match.fileId() + ":" + match.lineNo(),
                        k -> new TreeSet<>()).add(match.paymentId().value());
            }
        }
        TreeSet<String> challenged = new TreeSet<>();
        for (Break b : store.committedBreaks()) {
            if (b.type() == BreakType.INFERRED_MATCH_CHALLENGED) {
                challenged.add(b.merchantId() + "/" + b.subjectKey());
            }
        }
        for (Line l : store.committedLines()) {
            if (l.status() != LineStatus.INFERRED) {
                continue;
            }
            String key = l.merchantId() + "/" + l.key();
            TreeSet<String> paid = inferred.getOrDefault(key, new TreeSet<>());
            if (!paid.equals(truth.get(key)) && !challenged.contains(key)) {
                return Optional.of(new Violation(INFERRED_HONEST, key + " was inferred to pay " + paid
                        + ", but the world paid " + truth.get(key) + " in it, and nothing challenged it", now));
            }
        }
        return Optional.empty();
    }

    /**
     * A MISSING_SETTLEMENT the world implies for a payment that an unresolved unreferenced line could hold: an
     * ORPHAN_SETTLEMENT of the same merchant, settled within the lookback of the payment's batch date. Driftline
     * cannot tell whether the payment is inside that line, so it escalated the line instead of guessing (§6.4), which
     * INV-DETECTED accepts ("a break or an escalation", §11.7). The same rule as the matcher's.
     */
    private boolean coveredByUnresolvedBatch(String expectedKey, TreeSet<String> unresolvedLines) {
        if (!expectedKey.startsWith(BreakType.MISSING_SETTLEMENT + " ")) {
            return false;
        }
        String subject = expectedKey.substring(expectedKey.indexOf('/') + 1);
        Optional<FakeGateway.Payment> p = gateway.payment(PaymentId.of(subject));
        if (p.isEmpty()) {
            return false;
        }
        LocalDate batchDate = BatchMatching.batchDate(epoch.plusMillis(p.get().capturedAtMillis()), cycleDays);
        for (int d = 0; d <= LOOKBACK_DAYS; d++) {
            if (unresolvedLines.contains(p.get().merchant() + "/" + batchDate.plusDays(d))) {
                return true;
            }
        }
        return false;
    }

    /** The oracle's expected breaks, computed at the end of the run. */
    public TreeSet<String> expected() {
        return new TreeSet<>(expected);
    }

    /** Expected breaks Driftline escalated as an IDEMPOTENCY_CONFLICT on the order instead (D41). */
    public long coveredByIdempotencyConflict() {
        return coveredByIdempotencyConflict;
    }

    /** Expected MISSING_SETTLEMENT breaks held back because an unresolved batch line could hold the payment (D56). */
    public long coveredByUnresolvedBatch() {
        return coveredByUnresolvedBatch;
    }

    /** The oracle's expected breaks that Driftline raised. */
    public TreeSet<String> detected() {
        return new TreeSet<>(detected);
    }

    public Optional<Violation> atEnd(long now, SimLog log, long undeliveredWorldMessages) {
        Optional<Violation> step = afterStep(now);
        if (step.isPresent()) {
            return step;
        }
        for (int p = 0; p < log.partitionCount(); p++) {
            long committed = store.committedOffset(p);
            if (committed != log.endOffset(p)) {
                return Optional.of(new Violation(DRAINED, "partition " + p + " stopped at offset " + committed
                        + " of " + log.endOffset(p), now));
            }
        }
        if (undeliveredWorldMessages != 0) {
            return Optional.of(new Violation(DRAINED, undeliveredWorldMessages + " world messages were never acknowledged", now));
        }
        TreeSet<String> completeFiles = new TreeSet<>();
        for (SettlementRepo.FileEnd f : store.committedFileEnds()) {
            if (f.complete()) {
                completeFiles.add(f.merchantId() + "/" + f.fileId());
            }
        }
        TreeSet<String> unresolvedLines = new TreeSet<>();
        for (Line l : store.committedLines()) {
            if (l.status() == LineStatus.AWAITING_BATCH && completeFiles.contains(l.merchantId() + "/" + l.fileId())) {
                return Optional.of(new Violation(DRAINED, "line " + l.merchantId() + "/" + l.key()
                        + " of a complete file was never batch-matched", now));
            }
            if (l.status() == LineStatus.AMBIGUOUS) {
                Optional<Violation> v = witnessed(l.merchantId(), l.key(), now);
                if (v.isPresent()) {
                    return v;
                }
            }
            if (UNRESOLVED_BATCH.contains(l.status())) {
                unresolvedLines.add(l.merchantId() + "/" + l.settlementDate());
            }
        }
        if (gateway != null) {
            for (FakeGateway.Payment p : gateway.payments().values()) {
                if (!p.refunds().isEmpty() && fulfilledWith(p)) {
                    return Optional.of(new Violation(NO_REFUND_OF_FULFILLED, p.merchant() + "/" + p.id()
                            + " was refunded, and its order was fulfilled with it", now));
                }
            }
            Optional<Violation> dishonest = inferredHonest(now);   // before its consequences: the root cause first
            if (dishonest.isPresent()) {
                return dishonest;
            }
            Optional<Violation> late = deadlines(now);
            if (late.isPresent()) {
                return late;
            }
            Optional<Violation> whole = customersWhole(now);
            if (whole.isPresent()) {
                return whole;
            }
            Optional<Violation> books = booksMatchWorld(now);
            if (books.isPresent()) {
                return books;
            }
            expected = Oracle.expected(gateway, merchants, epoch, cycleDays, graceDays, lastFileDate);
            TreeSet<String> raised = new TreeSet<>();
            TreeSet<String> escalatedOrders = new TreeSet<>();
            for (Break b : store.committedBreaks()) {
                if (b.kind() == BreakKind.BREAK) {
                    raised.add(Oracle.key(b.type(), b.merchantId(), b.subjectKey()));
                }
                if (b.type() == BreakType.IDEMPOTENCY_CONFLICT && b.evidence().containsKey("order_id")) {
                    escalatedOrders.add(b.merchantId() + "/" + b.evidence().get("order_id"));
                }
            }
            detected = new TreeSet<>(expected);
            detected.retainAll(raised);
            for (String e : expected) {
                if (raised.contains(e)) {
                    continue;
                }
                if (coveredByEscalation(e, escalatedOrders)) {
                    coveredByIdempotencyConflict++;
                    continue;
                }
                if (coveredByUnresolvedBatch(e, unresolvedLines)) {
                    coveredByUnresolvedBatch++;
                    continue;
                }
                if (coveredByEscalatedLine(e, lineBreaks())) {
                    coveredByEscalatedLine++;
                    continue;
                }
                return Optional.of(new Violation(DETECTED, "the world implies " + e + ", but Driftline never raised it",
                        now));
            }
        }
        return Optional.empty();
    }
}
