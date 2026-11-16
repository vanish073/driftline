package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
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
import io.github.vanish073.driftline.core.order.OrderStatus;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

    /** A non-negative number as 19 digits, so that keys sort numerically. */
    private static String pad(long n) {
        String digits = Long.toString(n);
        return "0".repeat(19 - digits.length()) + digits;
    }

    private static Key breakKey(MerchantId m, String breakId) {
        return new Key(m.value(), breakId, "");
    }

    /**
     * A timer's place in a due index: grouped by clock and then by {@code group} (a partition number or a merchant
     * id), then in firing order. Range reads on it return exactly the due timers, already ordered, as PostgreSQL's
     * {@code timers_due} and {@code timers_due_by_merchant} indexes do.
     */
    private record DueKey(String clock, String group, Instant fireAt, String kind, String subject, String merchant)
            implements Comparable<DueKey> {

        private static final Comparator<DueKey> ORDER = Comparator.comparing(DueKey::clock)
                .thenComparing(DueKey::group)
                .thenComparing(DueKey::fireAt)
                .thenComparing(DueKey::kind)
                .thenComparing(DueKey::subject)
                .thenComparing(DueKey::merchant);

        static DueKey byPartition(TimerRepo.Timer t) {
            return new DueKey(t.clock().name(), pad(t.partition()), t.fireAt(), t.kind().name(), t.subjectKey(),
                    t.merchantId().value());
        }

        static DueKey byMerchant(TimerRepo.Timer t) {
            return new DueKey(t.clock().name(), t.merchantId().value(), t.fireAt(), t.kind().name(), t.subjectKey(),
                    t.merchantId().value());
        }

        /** The smallest key of a group, and the smallest key after every key with fireAt <= upTo. */
        static DueKey from(TimerRepo.TimerClock clock, String group) {
            return new DueKey(clock.name(), group, Instant.MIN, "", "", "");
        }

        static DueKey after(TimerRepo.TimerClock clock, String group, Instant upTo) {
            return new DueKey(clock.name(), group, upTo.plusNanos(1), "", "", "");
        }

        static DueKey groupEnd(TimerRepo.TimerClock clock, String group) {
            return new DueKey(clock.name(), group + "\u0000", Instant.MIN, "", "", "");
        }

        @Override
        public int compareTo(DueKey o) {
            return ORDER.compare(this, o);
        }
    }


    // Committed state.
    private final TreeMap<Key, LedgerTxn> ledger = new TreeMap<>();
    private final List<LedgerTxn> ledgerLog = new ArrayList<>();
    private final TreeMap<Key, Money> balances = new TreeMap<>();
    private final TreeMap<Key, Sha256> dedupe = new TreeMap<>();
    private final TreeMap<Key, PaymentState> payments = new TreeMap<>();
    /** Committed payments by (merchant, order, payment): the duplicate-capture join (§5.3). Orders never change. */
    private final TreeMap<Key, PaymentState> paymentsByOrder = new TreeMap<>();
    private final TreeMap<Key, List<PaymentTransition>> paymentTransitions = new TreeMap<>();
    private final List<Recorded<PaymentTransition>> paymentLog = new ArrayList<>();
    private final TreeMap<Key, OrderState> orders = new TreeMap<>();
    private final TreeMap<Key, List<OrderTransition>> orderTransitions = new TreeMap<>();
    private final List<Recorded<OrderTransition>> orderLog = new ArrayList<>();
    private final TreeMap<Integer, PartitionRepo.Lease> leases = new TreeMap<>();
    private final List<OffsetCommit> offsetCommitLog = new ArrayList<>();
    private final TreeMap<String, QuarantineRepo.Entry> quarantine = new TreeMap<>();
    private final List<QuarantineRepo.Entry> quarantineLog = new ArrayList<>();
    /** Undecodable records by quarantine id (one per log position, shared with {@link #quarantine}'s ids). */
    private final TreeMap<String, QuarantineRepo.Undecodable> undecodable = new TreeMap<>();
    private final Table<Key, WatermarkRepo.StreamWatermark> streamWatermarks = new Table<>();
    private final Table<Key, LocalDate> settledThrough = new Table<>();
    private final Table<Key, TimerRepo.Timer> timers = new Table<>();
    private final Table<DueKey, TimerRepo.Timer> timersByPartition = new Table<>();
    private final Table<DueKey, TimerRepo.Timer> timersByMerchant = new Table<>();
    private final Table<Key, Break> breaks = new Table<>();
    private final Table<Key, BreakTransition> breakTransitions = new Table<>();
    private final List<Break> breakRaiseLog = new ArrayList<>();
    private final Table<Key, SettlementRepo.ReceivedFile> receipts = new Table<>();
    private final Table<Key, SettlementRepo.Line> lines = new Table<>();
    private final Table<Key, SettlementRepo.FileEnd> fileEnds = new Table<>();
    private final Table<Key, SettlementRepo.Match> matches = new Table<>();
    private final Table<Key, String> actionOwners = new Table<>();
    private final Table<Key, BreakRepo.Plan> plans = new Table<>();
    private final Table<Key, OutboxRepo.Command> commands = new Table<>();
    private final Table<Key, AuditRepo.Row> auditRows = new Table<>();
    /** Every break transition after the first, in commit order (for incremental checks). */
    private final List<Recorded<BreakTransition>> breakTransitionLog = new ArrayList<>();

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

    /** Every committed break as it stands now, by merchant and break id. */
    public List<Break> committedBreaks() {
        return List.copyOf(breaks.committed().values());
    }

    /** Every break as it was when first raised, in commit order (for incremental checks). */
    public List<Break> committedBreakRaises() {
        return Collections.unmodifiableList(breakRaiseLog);
    }

    /** Every break transition after the first, in commit order. */
    public List<Recorded<BreakTransition>> committedBreakTransitions() {
        return Collections.unmodifiableList(breakTransitionLog);
    }

    /** The committed plan of a break, if the resolver has planned it. */
    public Optional<BreakRepo.Plan> committedPlan(MerchantId merchantId, String breakId) {
        return Optional.ofNullable(plans.committed().get(breakKey(merchantId, breakId)));
    }

    /** Every committed outbox command, by command id. */
    public List<OutboxRepo.Command> committedCommands() {
        return List.copyOf(commands.committed().values());
    }

    /** Every committed audit row, by sequence number. */
    public List<AuditRepo.Row> committedAuditRows() {
        return List.copyOf(auditRows.committed().values());
    }

    /** Every committed settlement match, by merchant and payment id. */
    public List<SettlementRepo.Match> committedMatches() {
        return List.copyOf(matches.committed().values());
    }

    /** Every committed settlement line, by merchant, file id and line number. */
    public List<SettlementRepo.Line> committedLines() {
        return List.copyOf(lines.committed().values());
    }

    /** Every committed upload receipt, by SHA-256. */
    public List<SettlementRepo.ReceivedFile> committedReceipts() {
        return List.copyOf(receipts.committed().values());
    }

    /** Every committed trailer, by merchant and file id. */
    public List<SettlementRepo.FileEnd> committedFileEnds() {
        return List.copyOf(fileEnds.committed().values());
    }

    /** The committed settlement watermark of a merchant, if any. */
    public Optional<LocalDate> committedSettledThrough(MerchantId merchantId) {
        return Optional.ofNullable(settledThrough.committed().get(new Key(merchantId.value(), "", "")));
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
        private final TreeMap<String, QuarantineRepo.Undecodable> pendingUndecodable = new TreeMap<>();
        private final Table<Key, WatermarkRepo.StreamWatermark>.View streamView = streamWatermarks.view();
        private final Table<Key, LocalDate>.View settledView = settledThrough.view();
        private final Table<Key, TimerRepo.Timer>.View timerView = timers.view();
        private final Table<DueKey, TimerRepo.Timer>.View timerByPartitionView = timersByPartition.view();
        private final Table<DueKey, TimerRepo.Timer>.View timerByMerchantView = timersByMerchant.view();
        private final Table<Key, Break>.View breakView = breaks.view();
        private final Table<Key, BreakTransition>.View breakTransitionView = breakTransitions.view();
        private final List<Break> pendingRaises = new ArrayList<>();
        private final Table<Key, SettlementRepo.ReceivedFile>.View receiptView = receipts.view();
        private final Table<Key, SettlementRepo.Line>.View lineView = lines.view();
        private final Table<Key, SettlementRepo.FileEnd>.View fileEndView = fileEnds.view();
        private final Table<Key, SettlementRepo.Match>.View matchView = matches.view();
        private final Table<Key, String>.View ownerView = actionOwners.view();
        private final Table<Key, BreakRepo.Plan>.View planView = plans.view();
        private final Table<Key, OutboxRepo.Command>.View commandView = commands.view();
        private final Table<Key, AuditRepo.Row>.View auditView = auditRows.view();
        private final List<Recorded<BreakTransition>> pendingBreakTransitions = new ArrayList<>();
        private final OutboxRepo outboxRepo = new InMemoryOutbox();
        private final AuditRepo auditRepo = new InMemoryAudit();

        private final LedgerRepo ledgerRepo = new InMemoryLedger();
        private final DedupeRepo dedupeRepo = new InMemoryDedupe();
        private final PaymentRepo paymentRepo = new InMemoryPayments();
        private final OrderRepo orderRepo = new InMemoryOrders();
        private final PartitionRepo partitionRepo = new InMemoryPartitions();
        private final QuarantineRepo quarantineRepo = new InMemoryQuarantine();
        private final WatermarkRepo watermarkRepo = new InMemoryWatermarks();
        private final TimerRepo timerRepo = new InMemoryTimers();
        private final BreakRepo breakRepo = new InMemoryBreaks();
        private final SettlementRepo settlementRepo = new InMemorySettlement();

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
            for (var e : pendingPayments.entrySet()) {
                PaymentState p = e.getValue();
                p.orderId().ifPresent(o -> paymentsByOrder.put(new Key(e.getKey().merchant(), o.value(),
                        p.paymentId().value()), p));
            }
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
            undecodable.putAll(pendingUndecodable);
            streamView.commit();
            settledView.commit();
            timerView.commit();
            timerByPartitionView.commit();
            timerByMerchantView.commit();
            breakView.commit();
            breakTransitionView.commit();
            breakRaiseLog.addAll(pendingRaises);
            receiptView.commit();
            lineView.commit();
            fileEndView.commit();
            matchView.commit();
            ownerView.commit();
            planView.commit();
            commandView.commit();
            auditView.commit();
            breakTransitionLog.addAll(pendingBreakTransitions);
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
            requireOpen();
            return watermarkRepo;
        }

        @Override
        public TimerRepo timers() {
            requireOpen();
            return timerRepo;
        }

        @Override
        public SettlementRepo settlement() {
            requireOpen();
            return settlementRepo;
        }

        @Override
        public BreakRepo breaks() {
            requireOpen();
            return breakRepo;
        }

        @Override
        public OutboxRepo outbox() {
            requireOpen();
            return outboxRepo;
        }

        @Override
        public AuditRepo audit() {
            requireOpen();
            return auditRepo;
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

            /**
             * The merchant's visible payments that match {@code filter} (committed, overlaid by this transaction's
             * writes), by payment id.
             */
            private List<PaymentState> visible(MerchantId merchantId, java.util.function.Predicate<PaymentState> filter) {
                String m = merchantId.value();
                TreeMap<Key, PaymentState> out = new TreeMap<>();
                for (var e : payments.subMap(new Key(m, "", ""), true, new Key(m + "\u0000", "", ""), false).entrySet()) {
                    if (!pendingPayments.containsKey(e.getKey()) && filter.test(e.getValue())) {
                        out.put(e.getKey(), e.getValue());
                    }
                }
                for (var e : pendingPayments.entrySet()) {
                    if (e.getKey().merchant().equals(m) && filter.test(e.getValue())) {
                        out.put(e.getKey(), e.getValue());
                    }
                }
                return new ArrayList<>(out.values());
            }

            @Override
            public List<PaymentState> forOrder(MerchantId merchantId, OrderId orderId) {
                requireOpen();
                String m = merchantId.value();
                TreeMap<String, PaymentState> out = new TreeMap<>();
                for (PaymentState p : paymentsByOrder.subMap(new Key(m, orderId.value(), ""), true,
                        new Key(m, orderId.value() + "\u0000", ""), false).values()) {
                    out.put(p.paymentId().value(), p);
                }
                for (var e : pendingPayments.entrySet()) {
                    if (e.getKey().merchant().equals(m) && e.getValue().orderId().filter(orderId::equals).isPresent()) {
                        out.put(e.getValue().paymentId().value(), e.getValue());
                    }
                }
                return List.copyOf(out.values());
            }

            @Override
            public List<PaymentState> unsettled(MerchantId merchantId) {
                requireOpen();
                return List.copyOf(visible(merchantId,
                        p -> matchView.get(paymentKey(merchantId, p.paymentId())).isEmpty()));
            }

            @Override
            public Optional<String> actionOwner(MerchantId merchantId, PaymentId paymentId) {
                requireOpen();
                return ownerView.get(paymentKey(merchantId, paymentId));
            }

            @Override
            public Optional<String> claimActionOwner(MerchantId merchantId, PaymentId paymentId, String breakId) {
                requireOpen();
                if (find(merchantId, paymentId).isEmpty()) {
                    throw new IllegalArgumentException("payment " + paymentId + " is not stored");
                }
                Optional<String> existing = ownerView.get(paymentKey(merchantId, paymentId));
                if (existing.isPresent()) {
                    return existing.get().equals(breakId) ? Optional.empty() : existing;
                }
                ownerView.put(paymentKey(merchantId, paymentId), breakId);
                return Optional.empty();
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

            @Override
            public List<OrderState> open(MerchantId merchantId) {
                requireOpen();
                TreeMap<Key, OrderState> out = new TreeMap<>();
                for (var e : orders.entrySet()) {
                    if (e.getKey().merchant().equals(merchantId.value())) {
                        out.put(e.getKey(), e.getValue());
                    }
                }
                for (var e : pendingOrders.entrySet()) {
                    if (e.getKey().merchant().equals(merchantId.value())) {
                        out.put(e.getKey(), e.getValue());
                    }
                }
                List<OrderState> open = new ArrayList<>();
                for (OrderState o : out.values()) {
                    if (o.status() == OrderStatus.CREATED || o.status() == OrderStatus.PENDING_PAYMENT
                            || o.status() == OrderStatus.PAID) {
                        open.add(o);
                    }
                }
                return List.copyOf(open);
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
            private boolean taken(String id) {
                return quarantine.containsKey(id) || pendingQuarantine.containsKey(id) || undecodable.containsKey(id)
                        || pendingUndecodable.containsKey(id);
            }

            @Override
            public void add(Entry entry) {
                requireOpen();
                Objects.requireNonNull(entry, "entry");
                if (!taken(entry.id())) {
                    pendingQuarantine.put(entry.id(), entry);
                }
            }

            @Override
            public void addUndecodable(Undecodable record) {
                requireOpen();
                Objects.requireNonNull(record, "record");
                if (!taken(record.id())) {
                    pendingUndecodable.put(record.id(), record);
                }
            }

            @Override
            public List<Undecodable> undecodable(int partition) {
                requireOpen();
                List<Undecodable> out = new ArrayList<>();
                for (Undecodable u : undecodable.values()) {
                    if (u.partition() == partition) {
                        out.add(u);
                    }
                }
                for (Undecodable u : pendingUndecodable.values()) {
                    if (u.partition() == partition) {
                        out.add(u);
                    }
                }
                out.sort(Comparator.comparingLong(Undecodable::offset));
                return List.copyOf(out);
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

        private final class InMemoryWatermarks implements WatermarkRepo {

            private Key streamKey(int partition, Source source) {
                return new Key("", pad(partition), source.name());
            }

            @Override
            public Optional<StreamWatermark> stream(int partition, Source source) {
                requireOpen();
                return streamView.get(streamKey(partition, source));
            }

            @Override
            public void saveStream(StreamWatermark w) {
                requireOpen();
                Key key = streamKey(w.partition(), w.source());
                Optional<StreamWatermark> current = streamView.get(key);
                if (current.isPresent()) {
                    StreamWatermark c = current.get();
                    if (w.value().isBefore(c.value())) {
                        throw new IllegalArgumentException("watermark of p" + w.partition() + "/" + w.source()
                                + " cannot move back");
                    }
                    if (c.maxEventTime().isPresent()
                            && (w.maxEventTime().isEmpty() || w.maxEventTime().get().isBefore(c.maxEventTime().get()))) {
                        throw new IllegalArgumentException("max event time of p" + w.partition() + "/" + w.source()
                                + " cannot move back");
                    }
                }
                streamView.put(key, w);
            }

            @Override
            public Optional<LocalDate> settledThrough(MerchantId merchantId) {
                requireOpen();
                return settledView.get(new Key(merchantId.value(), "", ""));
            }

            @Override
            public void saveSettledThrough(MerchantId merchantId, LocalDate date) {
                requireOpen();
                Objects.requireNonNull(date, "date");
                Key key = new Key(merchantId.value(), "", "");
                Optional<LocalDate> current = settledView.get(key);
                if (current.isPresent() && date.isBefore(current.get())) {
                    throw new IllegalArgumentException("settlement watermark of " + merchantId + " cannot move back");
                }
                settledView.put(key, date);
            }
        }

        private final class InMemoryTimers implements TimerRepo {

            private Key timerKey(MerchantId m, Kind kind, String subjectKey) {
                return new Key(m.value(), kind.name(), subjectKey);
            }

            private void unindex(Timer t) {
                timerByPartitionView.delete(DueKey.byPartition(t));
                timerByMerchantView.delete(DueKey.byMerchant(t));
            }

            @Override
            public void upsert(Timer timer) {
                requireOpen();
                Key key = timerKey(timer.merchantId(), timer.kind(), timer.subjectKey());
                timerView.get(key).ifPresent(this::unindex);
                timerView.put(key, timer);
                timerByPartitionView.put(DueKey.byPartition(timer), timer);
                timerByMerchantView.put(DueKey.byMerchant(timer), timer);
            }

            @Override
            public Optional<Timer> find(MerchantId merchantId, Kind kind, String subjectKey) {
                requireOpen();
                return timerView.get(timerKey(merchantId, kind, subjectKey));
            }

            @Override
            public List<Timer> due(int partition, TimerClock clock, Instant upTo) {
                requireOpen();
                String group = pad(partition);
                return List.copyOf(timerByPartitionView.range(DueKey.from(clock, group), DueKey.after(clock, group, upTo)));
            }

            @Override
            public List<Timer> dueForMerchant(MerchantId merchantId, TimerClock clock, Instant upTo) {
                requireOpen();
                String group = merchantId.value();
                return List.copyOf(timerByMerchantView.range(DueKey.from(clock, group), DueKey.after(clock, group, upTo)));
            }

            @Override
            public Optional<Instant> earliest(int partition, TimerClock clock) {
                requireOpen();
                String group = pad(partition);
                return timerByPartitionView.first(DueKey.from(clock, group), DueKey.groupEnd(clock, group))
                        .map(Timer::fireAt);
            }

            @Override
            public void delete(MerchantId merchantId, Kind kind, String subjectKey) {
                requireOpen();
                Key key = timerKey(merchantId, kind, subjectKey);
                Optional<Timer> existing = timerView.get(key);
                if (existing.isPresent()) {
                    unindex(existing.get());
                    timerView.delete(key);
                }
            }
        }

        private final class InMemoryBreaks implements BreakRepo {

            @Override
            public Raised raise(Break b, String reason) {
                requireOpen();
                Key key = breakKey(b.merchantId(), b.breakId());
                Optional<Break> existing = breakView.get(key);
                if (existing.isPresent()) {
                    return new Raised(false, existing.get());
                }
                BreakTransition first = new BreakTransition(b.breakId(), 1, Optional.empty(), b.state(), b.detectedAt(),
                        reason);
                breakView.put(key, b);
                breakTransitionView.put(new Key(b.merchantId().value(), b.breakId(), pad(1)), first);
                pendingRaises.add(b);
                return new Raised(true, b);
            }

            @Override
            public Optional<Break> find(MerchantId merchantId, BreakType type, String subjectKey) {
                requireOpen();
                return breakView.get(breakKey(merchantId, Break.idOf(merchantId, type, subjectKey)));
            }

            @Override
            public Optional<Break> byId(String breakId) {
                requireOpen();
                Objects.requireNonNull(breakId, "breakId");
                List<Break> found = breakView.where(b -> b.breakId().equals(breakId));
                return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
            }

            private Break existing(MerchantId merchantId, String breakId) {
                return breakView.get(breakKey(merchantId, breakId))
                        .orElseThrow(() -> new IllegalArgumentException("no break " + breakId));
            }

            @Override
            public void addEvidence(MerchantId merchantId, String breakId, String key, String value) {
                requireOpen();
                Break.requireEvidence(key, value);
                Break b = existing(merchantId, breakId);
                if (value.equals(b.evidence().get(key))) {
                    return;
                }
                TreeMap<String, String> evidence = new TreeMap<>(b.evidence());
                evidence.put(key, value);
                breakView.put(breakKey(merchantId, breakId), new Break(b.breakId(), b.merchantId(), b.type(),
                        b.subjectKey(), b.state(), b.detectedAt(), evidence, Math.addExact(b.version(), 1)));
            }

            @Override
            public void transition(MerchantId merchantId, String breakId, BreakState to, Instant at, String reason) {
                requireOpen();
                Break b = existing(merchantId, breakId);
                if (b.state() == to) {
                    throw new IllegalArgumentException("break " + breakId + " is already " + to);
                }
                int seq = transitions(merchantId, breakId).size() + 1;
                BreakTransition t = new BreakTransition(breakId, seq, Optional.of(b.state()), to, at, reason);
                breakTransitionView.put(new Key(merchantId.value(), breakId, pad(seq)), t);
                pendingBreakTransitions.add(new Recorded<>(merchantId, t));
                breakView.put(breakKey(merchantId, breakId), new Break(b.breakId(), b.merchantId(), b.type(),
                        b.subjectKey(), to, b.detectedAt(), b.evidence(), Math.addExact(b.version(), 1)));
            }

            @Override
            public List<Break> forMerchant(MerchantId merchantId) {
                requireOpen();
                List<Break> out = breakView.where(b -> b.merchantId().equals(merchantId));
                out.sort(Comparator.comparing((Break b) -> b.type().name()).thenComparing(Break::subjectKey));
                return List.copyOf(out);
            }

            @Override
            public List<BreakTransition> transitions(MerchantId merchantId, String breakId) {
                requireOpen();
                return List.copyOf(breakTransitionView.where(t -> t.breakId().equals(breakId)));
            }

            @Override
            public void savePlan(MerchantId merchantId, String breakId, Plan plan) {
                requireOpen();
                existing(merchantId, breakId);
                Optional<Plan> stored = planView.get(breakKey(merchantId, breakId));
                if (stored.flatMap(Plan::action).isPresent() && !stored.get().action().equals(plan.action())) {
                    throw new IllegalStateException("break " + breakId + " already has action "
                            + stored.get().action().get());
                }
                planView.put(breakKey(merchantId, breakId), plan);
            }

            @Override
            public Optional<Plan> planOf(MerchantId merchantId, String breakId) {
                requireOpen();
                return planView.get(breakKey(merchantId, breakId));
            }

            @Override
            public List<Break> dueForReview(Instant now, int limit) {
                requireOpen();
                List<Break> open = breakView.where(b -> b.state() == BreakState.DETECTED
                        || b.state() == BreakState.AWAITING_EVIDENCE || b.state() == BreakState.ACTION_PENDING);
                List<Break> due = new ArrayList<>();
                for (Break b : open) {
                    Optional<Plan> plan = planView.get(breakKey(b.merchantId(), b.breakId()));
                    if (plan.isEmpty() || !plan.get().reviewAt().isAfter(now)) {
                        due.add(b);
                    }
                }
                due.sort(Comparator.comparing((Break b) -> planView.get(breakKey(b.merchantId(), b.breakId()))
                                .map(Plan::reviewAt).orElse(Instant.MIN))
                        .thenComparing(Break::breakId));
                return List.copyOf(due.subList(0, Math.min(limit, due.size())));
            }

            @Override
            public void wake(MerchantId merchantId, String breakId, Instant at) {
                requireOpen();
                Key key = breakKey(merchantId, breakId);
                planView.get(key).filter(p -> p.reviewAt().isAfter(at.truncatedTo(ChronoUnit.MICROS)))
                        .ifPresent(p -> planView.put(key, p.withReviewAt(at)));
            }
        }

        private final class InMemoryOutbox implements OutboxRepo {

            private Key key(String commandId) {
                return new Key("", commandId, "");
            }

            @Override
            public boolean enqueue(Command command) {
                requireOpen();
                if (command.status() != CommandStatus.PENDING || command.attempt() != 0
                        || command.leaseOwner().isPresent()) {
                    throw new IllegalArgumentException("a new command is PENDING, unclaimed and unleased");
                }
                if (breakView.get(breakKey(command.merchantId(), command.breakId())).isEmpty()) {
                    throw new IllegalArgumentException("no break " + command.breakId());
                }
                if (commandView.get(key(command.commandId())).isPresent()) {
                    return false;
                }
                commandView.put(key(command.commandId()), command);
                return true;
            }

            @Override
            public Optional<Command> find(String commandId) {
                requireOpen();
                return commandView.get(key(commandId));
            }

            @Override
            public List<Command> forBreak(MerchantId merchantId, String breakId) {
                requireOpen();
                return List.copyOf(commandView.where(c -> c.merchantId().equals(merchantId)
                        && c.breakId().equals(breakId)));
            }

            @Override
            public List<Command> claim(Instant now, String owner, Instant leaseUntil, Instant retryAt, int limit) {
                requireOpen();
                List<Command> due = new ArrayList<>(commandView.where(c -> c.status() != CommandStatus.DONE
                        && !c.nextAttemptAt().isAfter(now)
                        && (c.leaseUntil().isEmpty() || c.leaseUntil().get().isBefore(now))));
                due.sort(Comparator.comparing(Command::nextAttemptAt).thenComparing(Command::commandId));
                List<Command> out = new ArrayList<>();
                for (Command c : due.subList(0, Math.min(limit, due.size()))) {
                    Command claimed = new Command(c.commandId(), c.merchantId(), c.breakId(), c.kind(), c.args(),
                            c.idempotencyKey(), CommandStatus.IN_FLIGHT, Math.addExact(c.attempt(), 1),
                            Optional.of(owner), Optional.of(leaseUntil), retryAt,
                            Optional.of(c.firstSentAt().orElse(now)), Optional.empty());
                    commandView.put(key(c.commandId()), claimed);
                    out.add(claimed);
                }
                return List.copyOf(out);
            }

            @Override
            public boolean complete(String commandId, Map<String, String> result) {
                requireOpen();
                Command c = commandView.get(key(commandId))
                        .orElseThrow(() -> new IllegalArgumentException("no command " + commandId));
                if (c.status() == CommandStatus.DONE) {
                    return false;
                }
                commandView.put(key(commandId), new Command(c.commandId(), c.merchantId(), c.breakId(), c.kind(),
                        c.args(), c.idempotencyKey(), CommandStatus.DONE, c.attempt(), Optional.empty(),
                        Optional.empty(), c.nextAttemptAt(), c.firstSentAt(), Optional.of(new TreeMap<>(result))));
                return true;
            }

            @Override
            public void retryAt(String commandId, Instant nextAttemptAt) {
                requireOpen();
                Command c = commandView.get(key(commandId))
                        .orElseThrow(() -> new IllegalArgumentException("no command " + commandId));
                if (c.status() == CommandStatus.DONE) {
                    return;
                }
                commandView.put(key(commandId), new Command(c.commandId(), c.merchantId(), c.breakId(), c.kind(),
                        c.args(), c.idempotencyKey(), CommandStatus.PENDING, c.attempt(), Optional.empty(),
                        Optional.empty(), nextAttemptAt, c.firstSentAt(), Optional.empty()));
            }
        }

        private final class InMemoryAudit implements AuditRepo {
            @Override
            public Row append(Entry entry) {
                requireOpen();
                List<Row> all = auditView.where(r -> true);
                String prev = all.isEmpty() ? GENESIS : all.get(all.size() - 1).hash();
                long seq = all.size() + 1L;
                Row row = new Row(seq, entry, prev, AuditRepo.hash(prev, entry));
                auditView.put(new Key("", pad(seq), ""), row);
                return row;
            }

            @Override
            public List<Row> rows() {
                requireOpen();
                return List.copyOf(auditView.where(r -> true));
            }
        }

        private final class InMemorySettlement implements SettlementRepo {

            @Override
            public Receipt receive(ReceivedFile file) {
                requireOpen();
                Key key = new Key("", file.sha256().hex(), "");
                if (receiptView.get(key).isPresent()) {
                    return Receipt.DUPLICATE;
                }
                boolean conflict = !receiptView.where(r -> r.merchantId().equals(file.merchantId())
                        && ((r.status() == ReceiptStatus.ACCEPTED && r.settlementDate().equals(file.settlementDate()))
                        || r.fileId().equals(file.fileId()))).isEmpty();
                ReceiptStatus status = conflict ? ReceiptStatus.REJECTED_CONFLICT : ReceiptStatus.ACCEPTED;
                receiptView.put(key, new ReceivedFile(file.merchantId(), file.fileId(), file.settlementDate(),
                        file.sha256(), file.lineCount(), file.totalAmount(), file.receivedAt(), status));
                return conflict ? Receipt.CONFLICT : Receipt.NEW;
            }

            @Override
            public Optional<ReceivedFile> receipt(Sha256 sha256) {
                requireOpen();
                return receiptView.get(new Key("", sha256.hex(), ""));
            }

            private Key lineKey(MerchantId m, String fileId, int lineNo) {
                return new Key(m.value(), fileId, pad(lineNo));
            }

            @Override
            public void saveLine(Line line) {
                requireOpen();
                lineView.put(lineKey(line.merchantId(), line.fileId(), line.lineNo()), line);
            }

            @Override
            public Optional<Line> line(MerchantId merchantId, String fileId, int lineNo) {
                requireOpen();
                return lineView.get(lineKey(merchantId, fileId, lineNo));
            }

            @Override
            public List<Line> lines(MerchantId merchantId, String fileId) {
                requireOpen();
                return List.copyOf(lineView.where(l -> l.merchantId().equals(merchantId) && l.fileId().equals(fileId)));
            }

            @Override
            public List<Line> linesReferencing(MerchantId merchantId, String reference) {
                requireOpen();
                return List.copyOf(lineView.where(l -> l.merchantId().equals(merchantId)
                        && l.reference().filter(reference::equals).isPresent()));
            }

            @Override
            public void saveFileEnd(FileEnd end) {
                requireOpen();
                fileEndView.put(new Key(end.merchantId().value(), end.fileId(), ""), end);
            }

            @Override
            public Optional<FileEnd> fileEnd(MerchantId merchantId, String fileId) {
                requireOpen();
                return fileEndView.get(new Key(merchantId.value(), fileId, ""));
            }

            @Override
            public Optional<FileEnd> completeFile(MerchantId merchantId, LocalDate settlementDate) {
                requireOpen();
                return fileEndView.where(e -> e.merchantId().equals(merchantId) && e.complete()
                        && e.settlementDate().equals(settlementDate)).stream().findFirst();
            }

            @Override
            public Optional<Match> match(Match match) {
                requireOpen();
                Key key = paymentKey(match.merchantId(), match.paymentId());
                Optional<Match> existing = matchView.get(key);
                if (existing.isPresent()) {
                    return existing;
                }
                matchView.put(key, match);
                return Optional.empty();
            }

            @Override
            public Optional<Match> matchOf(MerchantId merchantId, PaymentId paymentId) {
                requireOpen();
                return matchView.get(paymentKey(merchantId, paymentId));
            }
        }
    }
}
