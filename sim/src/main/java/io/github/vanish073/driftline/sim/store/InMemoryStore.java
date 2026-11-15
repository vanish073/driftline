package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerChecks;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.order.OrderState;
import io.github.vanish073.driftline.core.order.OrderTransition;
import io.github.vanish073.driftline.core.payment.PaymentState;
import io.github.vanish073.driftline.core.payment.PaymentTransition;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.BreakRepo;
import io.github.vanish073.driftline.core.port.DedupeRepo;
import io.github.vanish073.driftline.core.port.LedgerRepo;
import io.github.vanish073.driftline.core.port.OrderRepo;
import io.github.vanish073.driftline.core.port.OutboxRepo;
import io.github.vanish073.driftline.core.port.PartitionRepo;
import io.github.vanish073.driftline.core.port.PaymentRepo;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.port.TimerRepo;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.core.port.WatermarkRepo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The simulator's {@link Store} (DESIGN §3.2, §11.3). Transactions run one at a time. Each one buffers its writes and
 * applies them at commit in one step, so a body that throws (a crash, a rejected posting) leaves nothing behind.
 * Committed state lives here, outside every simulated node, so it survives node crashes. Every map is ordered, so
 * nothing about it depends on hash codes (§11.11).
 *
 * <p>What it cannot model, stated in DESIGN §11.9: concurrency and isolation anomalies, SQL constraints and
 * triggers. The Store contract suite and the PostgreSQL integration tests cover those.
 *
 * <p>Two canaries remove a check that PostgreSQL makes with a constraint or a WHERE clause (DESIGN §11.10):
 * C05 ({@link Canary#LEDGER_UNIQUE_OFF}) drops the ledger's cause uniqueness, and C09 ({@link Canary#NO_FENCING})
 * drops the epoch check on offset commits. Every offset commit is logged with the epoch it used and the epoch the
 * lease had, so INV-FENCED is checked against what happened, not against the check itself.
 */
public final class InMemoryStore implements Store {

    private static final WatermarkRepo WATERMARKS = new WatermarkRepo() { };
    private static final TimerRepo TIMERS = new TimerRepo() { };
    private static final SettlementRepo SETTLEMENT = new SettlementRepo() { };
    private static final BreakRepo BREAKS = new BreakRepo() { };
    private static final OutboxRepo OUTBOX = new OutboxRepo() { };
    private static final AuditRepo AUDIT = new AuditRepo() { };

    /** A committed row tagged with its merchant, for the simulator's invariants. */
    public record Recorded<T>(MerchantId merchantId, T row) {
    }

    /** One committed offset commit: the epoch the committer used and the lease's epoch when it committed. */
    public record OffsetCommit(String txName, int partition, long epochUsed, long leaseEpoch, long nextOffset) {
    }

    private record Key(String merchant, String a, String b) implements Comparable<Key> {
        @Override
        public int compareTo(Key o) {
            int c = merchant.compareTo(o.merchant);
            if (c == 0) {
                c = a.compareTo(o.a);
            }
            return c != 0 ? c : b.compareTo(o.b);
        }
    }

    private static Key causeKey(MerchantId m, Cause cause) {
        return new Key(m.value(), cause.type().name(), cause.id());
    }

    private static Key accountKey(MerchantId m, AccountCode code) {
        return new Key(m.value(), code.name(), "");
    }

    private static Key paymentKey(MerchantId m, PaymentId id) {
        return new Key(m.value(), id.value(), "");
    }

    private static Key orderKey(MerchantId m, OrderId id) {
        return new Key(m.value(), id.value(), "");
    }

    private static Key dedupeKey(MerchantId m, Source source, String eventKey) {
        return new Key(m.value(), source.name(), eventKey);
    }

    // Committed state.
    private final TreeMap<Key, LedgerTxn> ledger = new TreeMap<>();
    private final List<LedgerTxn> ledgerLog = new ArrayList<>();
    private final TreeMap<Key, Money> balances = new TreeMap<>();
    private final TreeMap<Key, Sha256> dedupe = new TreeMap<>();
    private final TreeMap<Key, PaymentState> payments = new TreeMap<>();
    private final TreeMap<Key, List<PaymentTransition>> paymentTransitions = new TreeMap<>();
    private final List<Recorded<PaymentTransition>> paymentLog = new ArrayList<>();
    private final TreeMap<Key, OrderState> orders = new TreeMap<>();
    private final TreeMap<Key, List<OrderTransition>> orderTransitions = new TreeMap<>();
    private final List<Recorded<OrderTransition>> orderLog = new ArrayList<>();
    private final TreeMap<Integer, PartitionRepo.Lease> leases = new TreeMap<>();
    private final List<OffsetCommit> offsetCommitLog = new ArrayList<>();
    private final TreeMap<String, QuarantineRepo.Entry> quarantine = new TreeMap<>();
    private final List<QuarantineRepo.Entry> quarantineLog = new ArrayList<>();

    private final boolean ledgerUniqueOff;
    private final boolean noFencing;
    private boolean unavailable;
    private boolean inTransaction;
    private long commits;

    /** A store with every check in place. */
    public InMemoryStore() {
        this(Canaries.NONE);
    }

    public InMemoryStore(Canaries canaries) {
        this.ledgerUniqueOff = canaries.isEnabled(Canary.LEDGER_UNIQUE_OFF);
        this.noFencing = canaries.isEnabled(Canary.NO_FENCING);
    }

    /** Switches the STORE_DOWN fault on or off (DESIGN §11.5): while on, every transaction fails before it runs. */
    public void setUnavailable(boolean unavailable) {
        this.unavailable = unavailable;
    }

    @Override
    public <R> R transact(String txName, TxBody<R> body) {
        Objects.requireNonNull(txName, "txName");
        Objects.requireNonNull(body, "body");
        if (unavailable) {
            throw new StoreUnavailable("in-memory store is down (STORE_DOWN) for " + txName);
        }
        if (inTransaction) {
            throw new IllegalStateException("transactions do not nest: " + txName + " started inside another");
        }
        inTransaction = true;
        InMemoryTx tx = new InMemoryTx(txName);
        try {
            R result = body.run(tx);
            tx.commit();
            commits++;
            return result;
        } finally {
            tx.open = false;
            inTransaction = false;
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Read-only views for the simulator's invariants (not part of the Store port)
    // ---------------------------------------------------------------------------------------------------------------

    /** Every committed ledger transaction, in commit order. */
    public List<LedgerTxn> committedLedger() {
        return Collections.unmodifiableList(ledgerLog);
    }

    /** Every committed payment transition, in commit order. */
    public List<Recorded<PaymentTransition>> committedPaymentTransitions() {
        return Collections.unmodifiableList(paymentLog);
    }

    /** Every committed order transition, in commit order. */
    public List<Recorded<OrderTransition>> committedOrderTransitions() {
        return Collections.unmodifiableList(orderLog);
    }

    /** The committed next offset of a partition, or 0. */
    public long committedOffset(int partition) {
        return lease(partition).nextOffset();
    }

    /** Every committed offset commit, in commit order (INV-FENCED). */
    public List<OffsetCommit> committedOffsetCommits() {
        return Collections.unmodifiableList(offsetCommitLog);
    }

    /** Every committed quarantine row, in commit order. */
    public List<QuarantineRepo.Entry> committedQuarantine() {
        return Collections.unmodifiableList(quarantineLog);
    }

    private PartitionRepo.Lease lease(int partition) {
        PartitionRepo.Lease lease = leases.get(partition);
        return lease != null ? lease : new PartitionRepo.Lease(partition, 0, Optional.empty(), 0);
    }

    public long commits() {
        return commits;
    }

    // ---------------------------------------------------------------------------------------------------------------

    private final class InMemoryTx implements Tx {

        private final String txName;
        private boolean open = true;
        private final TreeMap<Key, LedgerTxn> pendingLedger = new TreeMap<>();
        private final List<LedgerTxn> pendingLedgerOrder = new ArrayList<>();
        private final TreeMap<Key, Sha256> pendingDedupe = new TreeMap<>();
        private final TreeMap<Key, PaymentState> pendingPayments = new TreeMap<>();
        private final List<Recorded<PaymentTransition>> pendingPaymentLog = new ArrayList<>();
        private final TreeMap<Key, OrderState> pendingOrders = new TreeMap<>();
        private final List<Recorded<OrderTransition>> pendingOrderLog = new ArrayList<>();
        private final TreeMap<Integer, PartitionRepo.Lease> pendingLeases = new TreeMap<>();
        private final List<OffsetCommit> pendingOffsetCommits = new ArrayList<>();
        private final TreeMap<String, QuarantineRepo.Entry> pendingQuarantine = new TreeMap<>();

        private final LedgerRepo ledgerRepo = new InMemoryLedger();
        private final DedupeRepo dedupeRepo = new InMemoryDedupe();
        private final PaymentRepo paymentRepo = new InMemoryPayments();
        private final OrderRepo orderRepo = new InMemoryOrders();
        private final PartitionRepo partitionRepo = new InMemoryPartitions();
        private final QuarantineRepo quarantineRepo = new InMemoryQuarantine();

        InMemoryTx(String txName) {
            this.txName = txName;
        }

        private void requireOpen() {
            if (!open) {
                throw new IllegalStateException("this Tx belongs to a transaction that has ended");
            }
        }

        private void commit() {
            for (LedgerTxn txn : pendingLedgerOrder) {
                ledger.put(causeKey(txn.merchantId(), txn.cause()), txn);
                ledgerLog.add(txn);
                for (Entry entry : txn.entries()) {
                    balances.merge(accountKey(entry.merchantId(), entry.account()), entry.amount(), Money::plus);
                }
            }
            dedupe.putAll(pendingDedupe);
            payments.putAll(pendingPayments);
            for (Recorded<PaymentTransition> r : pendingPaymentLog) {
                paymentTransitions.computeIfAbsent(paymentKey(r.merchantId(), r.row().paymentId()), k -> new ArrayList<>())
                        .add(r.row());
                paymentLog.add(r);
            }
            orders.putAll(pendingOrders);
            for (Recorded<OrderTransition> r : pendingOrderLog) {
                orderTransitions.computeIfAbsent(orderKey(r.merchantId(), r.row().orderId()), k -> new ArrayList<>())
                        .add(r.row());
                orderLog.add(r);
            }
            leases.putAll(pendingLeases);
            offsetCommitLog.addAll(pendingOffsetCommits);
            for (QuarantineRepo.Entry entry : pendingQuarantine.values()) {
                if (quarantine.putIfAbsent(entry.id(), entry) == null) {
                    quarantineLog.add(entry);
                }
            }
        }

        @Override
        public LedgerRepo ledger() {
            requireOpen();
            return ledgerRepo;
        }

        @Override
        public DedupeRepo dedupe() {
            requireOpen();
            return dedupeRepo;
        }

        @Override
        public PaymentRepo payments() {
            requireOpen();
            return paymentRepo;
        }

        @Override
        public OrderRepo orders() {
            requireOpen();
            return orderRepo;
        }

        @Override
        public PartitionRepo partitions() {
            requireOpen();
            return partitionRepo;
        }

        @Override
        public QuarantineRepo quarantine() {
            requireOpen();
            return quarantineRepo;
        }

        @Override
        public WatermarkRepo watermarks() {
            return WATERMARKS;
        }

        @Override
        public TimerRepo timers() {
            return TIMERS;
        }

        @Override
        public SettlementRepo settlement() {
            return SETTLEMENT;
        }

        @Override
        public BreakRepo breaks() {
            return BREAKS;
        }

        @Override
        public OutboxRepo outbox() {
            return OUTBOX;
        }

        @Override
        public AuditRepo audit() {
            return AUDIT;
        }

        private final class InMemoryLedger implements LedgerRepo {
            @Override
            public PostResult post(LedgerTxn txn) {
                requireOpen();
                LedgerChecks.requireValid(txn.merchantId(), txn.entries());
                Key key = causeKey(txn.merchantId(), txn.cause());
                Optional<LedgerTxn> existing = ledgerUniqueOff ? Optional.empty() : lookup(key);
                if (existing.isPresent()) {
                    return existing.get().sameEntriesAs(txn)
                            ? new PostResult.Duplicate(existing.get().id())
                            : new PostResult.LedgerConflict(existing.get(), txn);
                }
                pendingLedger.put(key, txn);
                pendingLedgerOrder.add(txn);
                return new PostResult.Posted(txn.id());
            }

            @Override
            public Optional<LedgerTxn> find(MerchantId merchantId, Cause cause) {
                requireOpen();
                return lookup(causeKey(merchantId, cause));
            }

            @Override
            public Money balance(MerchantId merchantId, AccountCode account) {
                requireOpen();
                Money total = balances.getOrDefault(accountKey(merchantId, account), Money.ZERO);
                for (LedgerTxn txn : pendingLedgerOrder) {
                    for (Entry entry : txn.entries()) {
                        if (entry.merchantId().equals(merchantId) && entry.account() == account) {
                            total = total.plus(entry.amount());
                        }
                    }
                }
                return total;
            }

            private Optional<LedgerTxn> lookup(Key key) {
                LedgerTxn pending = pendingLedger.get(key);
                return pending != null ? Optional.of(pending) : Optional.ofNullable(ledger.get(key));
            }
        }

        private final class InMemoryDedupe implements DedupeRepo {
            @Override
            public Claim claim(MerchantId merchantId, Source source, String eventKey, Sha256 payloadSha256,
                               int partition, long offset) {
                requireOpen();
                Key key = dedupeKey(merchantId, source, eventKey);
                Sha256 seen = pendingDedupe.containsKey(key) ? pendingDedupe.get(key) : dedupe.get(key);
                if (seen != null) {
                    return seen.equals(payloadSha256) ? Claim.DUPLICATE : Claim.KEY_REUSED;
                }
                pendingDedupe.put(key, payloadSha256);
                return Claim.FIRST;
            }
        }

        private final class InMemoryPayments implements PaymentRepo {
            @Override
            public Optional<PaymentState> find(MerchantId merchantId, PaymentId paymentId) {
                requireOpen();
                Key key = paymentKey(merchantId, paymentId);
                PaymentState pending = pendingPayments.get(key);
                return pending != null ? Optional.of(pending) : Optional.ofNullable(payments.get(key));
            }

            @Override
            public void save(MerchantId merchantId, PaymentState state) {
                requireOpen();
                pendingPayments.put(paymentKey(merchantId, state.paymentId()), state);
            }

            @Override
            public void appendTransition(MerchantId merchantId, PaymentTransition transition) {
                requireOpen();
                pendingPaymentLog.add(new Recorded<>(merchantId, transition));
            }

            @Override
            public List<PaymentTransition> transitions(MerchantId merchantId, PaymentId paymentId) {
                requireOpen();
                List<PaymentTransition> all = new ArrayList<>(
                        paymentTransitions.getOrDefault(paymentKey(merchantId, paymentId), List.of()));
                for (Recorded<PaymentTransition> r : pendingPaymentLog) {
                    if (r.merchantId().equals(merchantId) && r.row().paymentId().equals(paymentId)) {
                        all.add(r.row());
                    }
                }
                return List.copyOf(all);
            }
        }

        private final class InMemoryOrders implements OrderRepo {
            @Override
            public Optional<OrderState> find(MerchantId merchantId, OrderId orderId) {
                requireOpen();
                Key key = orderKey(merchantId, orderId);
                OrderState pending = pendingOrders.get(key);
                return pending != null ? Optional.of(pending) : Optional.ofNullable(orders.get(key));
            }

            @Override
            public void save(MerchantId merchantId, OrderState state) {
                requireOpen();
                pendingOrders.put(orderKey(merchantId, state.orderId()), state);
            }

            @Override
            public void appendTransition(MerchantId merchantId, OrderTransition transition) {
                requireOpen();
                pendingOrderLog.add(new Recorded<>(merchantId, transition));
            }

            @Override
            public List<OrderTransition> transitions(MerchantId merchantId, OrderId orderId) {
                requireOpen();
                List<OrderTransition> all = new ArrayList<>(
                        orderTransitions.getOrDefault(orderKey(merchantId, orderId), List.of()));
                for (Recorded<OrderTransition> r : pendingOrderLog) {
                    if (r.merchantId().equals(merchantId) && r.row().orderId().equals(orderId)) {
                        all.add(r.row());
                    }
                }
                return List.copyOf(all);
            }
        }

        private final class InMemoryPartitions implements PartitionRepo {
            @Override
            public Lease acquire(int partition, String owner) {
                requireOpen();
                Lease current = lease(partition);
                Lease next = new Lease(partition, Math.addExact(current.epoch(), 1), Optional.of(owner),
                        current.nextOffset());
                pendingLeases.put(partition, next);
                return next;
            }

            @Override
            public Lease lease(int partition) {
                requireOpen();
                Lease pending = pendingLeases.get(partition);
                return pending != null ? pending : InMemoryStore.this.lease(partition);
            }

            @Override
            public long nextOffset(int partition) {
                return lease(partition).nextOffset();
            }

            @Override
            public void commitOffset(int partition, long epoch, long nextOffset) {
                requireOpen();
                Lease current = lease(partition);
                if (epoch != current.epoch() && !noFencing) {
                    throw new StoreConflict("partition " + partition + " is fenced: epoch " + epoch
                            + " was superseded by " + current.epoch());
                }
                if (nextOffset < current.nextOffset()) {
                    throw new IllegalArgumentException("offset of partition " + partition + " cannot move back from "
                            + current.nextOffset() + " to " + nextOffset);
                }
                pendingLeases.put(partition, new Lease(partition, current.epoch(), current.owner(), nextOffset));
                pendingOffsetCommits.add(new OffsetCommit(txName, partition, epoch, current.epoch(), nextOffset));
            }
        }

        private final class InMemoryQuarantine implements QuarantineRepo {
            @Override
            public void add(Entry entry) {
                requireOpen();
                Objects.requireNonNull(entry, "entry");
                if (!quarantine.containsKey(entry.id())) {
                    pendingQuarantine.putIfAbsent(entry.id(), entry);
                }
            }

            @Override
            public List<Entry> forMerchant(MerchantId merchantId) {
                requireOpen();
                List<Entry> out = new ArrayList<>();
                for (Entry e : quarantine.values()) {
                    if (e.merchantId().equals(merchantId)) {
                        out.add(e);
                    }
                }
                for (Entry e : pendingQuarantine.values()) {
                    if (e.merchantId().equals(merchantId)) {
                        out.add(e);
                    }
                }
                out.sort(Comparator.comparingInt(Entry::partition).thenComparingLong(Entry::offset));
                return List.copyOf(out);
            }
        }
    }
}
