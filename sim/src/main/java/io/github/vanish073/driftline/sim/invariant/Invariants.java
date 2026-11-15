package io.github.vanish073.driftline.sim.invariant;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakKind;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.sim.world.FakeBank;
import io.github.vanish073.driftline.sim.world.FakeGateway;
import io.github.vanish073.driftline.sim.world.FakeMerchant;
import io.github.vanish073.driftline.sim.world.SettlementFeed;
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
 * The invariants of DESIGN §11.7 checked so far (Shifts 3 and 4). Incremental checks run after every step and look
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

    /**
     * Anomalies the simulated world never causes, so Driftline raising one is a bug. FILE_CONFLICT is expected only
     * for a merchant the bank sent a conflicting file for; IDEMPOTENCY_CONFLICT only with a quarantined record, which
     * the quarantine check above ties to a key the world really reused; FEED_STALL whenever a file is late.
     */
    static final List<BreakType> NEVER_EXPECTED = List.of(BreakType.STATUS_CONFLICT, BreakType.ORDER_ANOMALY,
            BreakType.OVER_SETTLEMENT, BreakType.SETTLEMENT_CONFLICT, BreakType.INFERRED_MATCH_CHALLENGED,
            BreakType.LEDGER_CONFLICT);

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
            Optional<Violation> v = checkRaised(raises.get(breakSeen), now);
            if (v.isPresent()) {
                return v;
            }
        }
        if (bank.total() != bank.opened()) {
            return Optional.of(new Violation(WORLD_CONSERVED, "the world holds " + bank.total() + " paise but "
                    + bank.opened() + " were opened", now));
        }
        return Optional.empty();
    }

    private Optional<Violation> checkRaised(Break b, long now) {
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

    /** The oracle's expected breaks, computed at the end of the run. */
    public TreeSet<String> expected() {
        return new TreeSet<>(expected);
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
        if (gateway != null) {
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
                if (!raised.contains(e) && !coveredByEscalation(e, escalatedOrders)) {
                    return Optional.of(new Violation(DETECTED, "the world implies " + e + ", but Driftline never raised it",
                            now));
                }
            }
        }
        return Optional.empty();
    }
}
