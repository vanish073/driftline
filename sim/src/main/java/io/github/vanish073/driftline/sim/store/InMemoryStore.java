package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
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
import io.github.vanish073.driftline.core.port.SettlementRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.port.TimerRepo;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.TxBody;
import io.github.vanish073.driftline.core.port.WatermarkRepo;
import java.util.ArrayList;
import java.util.Collections;
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
    private final TreeMap<Integer, Long> offsets = new TreeMap<>();

    private boolean unavailable;
    private boolean inTransaction;
    private long commits;

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
        InMemoryTx tx = new InMemoryTx();
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
        return offsets.getOrDefault(partition, 0L);
    }

    public long commits() {
        return commits;
    }

    // ---------------------------------------------------------------------------------------------------------------

    private final class InMemoryTx implements Tx {

        private boolean open = true;
        private final TreeMap<Key, LedgerTxn> pendingLedger = new TreeMap<>();
        private final List<LedgerTxn> pendingLedgerOrder = new ArrayList<>();
        private final TreeMap<Key, Sha256> pendingDedupe = new TreeMap<>();
        private final TreeMap<Key, PaymentState> pendingPayments = new TreeMap<>();
        private final List<Recorded<PaymentTransition>> pendingPaymentLog = new ArrayList<>();
        private final TreeMap<Key, OrderState> pendingOrders = new TreeMap<>();
        private final List<Recorded<OrderTransition>> pendingOrderLog = new ArrayList<>();
        private final TreeMap<Integer, Long> pendingOffsets = new TreeMap<>();

        private final LedgerRepo ledgerRepo = new InMemoryLedger();
        private final DedupeRepo dedupeRepo = new InMemoryDedupe();
        private final PaymentRepo paymentRepo = new InMemoryPayments();
        private final OrderRepo orderRepo = new InMemoryOrders();
        private final PartitionRepo partitionRepo = new InMemoryPartitions();

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
            offsets.putAll(pendingOffsets);
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
                Optional<LedgerTxn> existing = lookup(key);
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
            public long nextOffset(int partition) {
                requireOpen();
                Long pending = pendingOffsets.get(partition);
                return pending != null ? pending : offsets.getOrDefault(partition, 0L);
            }

            @Override
            public void commitOffset(int partition, long nextOffset) {
                requireOpen();
                long current = nextOffset(partition);
                if (nextOffset < current) {
                    throw new IllegalArgumentException("offset of partition " + partition + " cannot move back from "
                            + current + " to " + nextOffset);
                }
                pendingOffsets.put(partition, nextOffset);
            }
        }
    }
}
