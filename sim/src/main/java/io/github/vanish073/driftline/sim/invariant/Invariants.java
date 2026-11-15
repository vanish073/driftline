package io.github.vanish073.driftline.sim.invariant;

import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentStatus;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.store.InMemoryStore;
import io.github.vanish073.driftline.sim.world.FakeBank;
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

    /** @param reusedKeys the keys the world's buggy clients reused (MERCHANT_KEY_REUSE), per merchant and key */
    public Invariants(InMemoryStore store, FakeBank bank, Supplier<Set<String>> reusedKeys) {
        this.store = store;
        this.bank = bank;
        this.reusedKeys = reusedKeys;
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
        if (bank.total() != bank.opened()) {
            return Optional.of(new Violation(WORLD_CONSERVED, "the world holds " + bank.total() + " paise but "
                    + bank.opened() + " were opened", now));
        }
        return Optional.empty();
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
        return Optional.empty();
    }
}
