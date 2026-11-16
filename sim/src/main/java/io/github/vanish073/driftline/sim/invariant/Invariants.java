package io.github.vanish073.driftline.sim.invariant;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakKind;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.matcher.MatcherConfig;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo.Line;
import io.github.vanish073.driftline.core.port.SettlementRepo.LineStatus;
import io.github.vanish073.driftline.core.settlement.BatchMatching;
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

    /** Unreferenced lines whose money is still in suspense, escalated as ORPHAN_SETTLEMENT (§6.4). */
    static final List<LineStatus> UNRESOLVED_BATCH = List.of(LineStatus.AMBIGUOUS, LineStatus.NO_SOLUTION,
            LineStatus.BUDGET_EXCEEDED, LineStatus.OUT_OF_RANGE);
    static final int LOOKBACK_DAYS = MatcherConfig.DEFAULT.batchLookbackDays();

    /**
     * Anomalies the simulated world never causes, so Driftline raising one is a bug. FILE_CONFLICT is expected only
     * for a merchant the bank sent a conflicting file for; IDEMPOTENCY_CONFLICT only with a quarantined record, which
     * the quarantine check above ties to a key the world really reused; FEED_STALL whenever a file is late.
     * INFERRED_MATCH_CHALLENGED is possible from Shift 7: a success delivered after its batch line was solved.
     */
    static final List<BreakType> NEVER_EXPECTED = List.of(BreakType.STATUS_CONFLICT, BreakType.ORDER_ANOMALY,
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
    private long coveredByUnresolvedBatch;

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
     * nets (the contracted fee model) sum to the line's amount.
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
        for (List<PaymentId> witness : w) {
            long sum = 0;
            TreeSet<String> distinct = new TreeSet<>();
            for (PaymentId id : witness) {
                Optional<FakeGateway.Payment> p = gateway.payment(id);
                if (p.isEmpty() || !p.get().captured() || !distinct.add(id.value())) {
                    return Optional.of(new Violation(AMBIGUITY_WITNESSED, m + "/" + lineKey + " witness " + witness
                            + " names " + id + ", not a distinct captured payment", now));
                }
                LocalDate batchDate = BatchMatching.batchDate(epoch.plusMillis(p.get().capturedAtMillis()), cycleDays);
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
            Optional<Violation> dishonest = inferredHonest(now);
            if (dishonest.isPresent()) {
                return dishonest;
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
                return Optional.of(new Violation(DETECTED, "the world implies " + e + ", but Driftline never raised it",
                        now));
            }
        }
        return Optional.empty();
    }
}
