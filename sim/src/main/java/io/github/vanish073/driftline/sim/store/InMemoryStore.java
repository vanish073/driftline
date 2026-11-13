package io.github.vanish073.driftline.sim.store;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.ledger.AccountCode;
import io.github.vanish073.driftline.core.ledger.Cause;
import io.github.vanish073.driftline.core.ledger.Entry;
import io.github.vanish073.driftline.core.ledger.LedgerChecks;
import io.github.vanish073.driftline.core.ledger.LedgerTxn;
import io.github.vanish073.driftline.core.ledger.PostResult;
import io.github.vanish073.driftline.core.money.Money;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The simulator's {@link Store} (DESIGN §3.2, §11.3). Transactions run one at a time. Each one buffers its writes and
 * applies them at commit in one step, so a body that throws (a crash, a rejected posting) leaves nothing behind.
 * Committed state lives here, outside every simulated node, so it survives node crashes.
 *
 * <p>What it cannot model, stated in DESIGN §11.9: concurrency and isolation anomalies, SQL constraints and
 * triggers. The Store contract suite and the PostgreSQL integration tests cover those.
 */
public final class InMemoryStore implements Store {

    private static final DedupeRepo DEDUPE = new DedupeRepo() { };
    private static final OrderRepo ORDERS = new OrderRepo() { };
    private static final PaymentRepo PAYMENTS = new PaymentRepo() { };
    private static final WatermarkRepo WATERMARKS = new WatermarkRepo() { };
    private static final TimerRepo TIMERS = new TimerRepo() { };
    private static final SettlementRepo SETTLEMENT = new SettlementRepo() { };
    private static final BreakRepo BREAKS = new BreakRepo() { };
    private static final OutboxRepo OUTBOX = new OutboxRepo() { };
    private static final PartitionRepo PARTITIONS = new PartitionRepo() { };
    private static final AuditRepo AUDIT = new AuditRepo() { };

    private record CauseKey(MerchantId merchantId, Cause cause) implements Comparable<CauseKey> {
        static CauseKey of(LedgerTxn txn) {
            return new CauseKey(txn.merchantId(), txn.cause());
        }

        @Override
        public int compareTo(CauseKey other) {
            int byMerchant = merchantId.compareTo(other.merchantId);
            return byMerchant != 0 ? byMerchant : cause.compareTo(other.cause);
        }
    }

    private record AccountKey(MerchantId merchantId, AccountCode account) implements Comparable<AccountKey> {
        @Override
        public int compareTo(AccountKey other) {
            int byMerchant = merchantId.compareTo(other.merchantId);
            return byMerchant != 0 ? byMerchant : account.compareTo(other.account);
        }
    }

    private final TreeMap<CauseKey, LedgerTxn> ledger = new TreeMap<>();
    private final TreeMap<AccountKey, Money> balances = new TreeMap<>();
    private boolean unavailable;
    private boolean inTransaction;

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
            return result;
        } finally {
            tx.open = false;
            inTransaction = false;
        }
    }

    private final class InMemoryTx implements Tx, LedgerRepo {

        private boolean open = true;
        private final TreeMap<CauseKey, LedgerTxn> pendingLedger = new TreeMap<>();
        private final List<LedgerTxn> pendingOrder = new ArrayList<>();

        private void commit() {
            for (LedgerTxn txn : pendingOrder) {
                ledger.put(CauseKey.of(txn), txn);
                for (Entry entry : txn.entries()) {
                    balances.merge(new AccountKey(entry.merchantId(), entry.account()), entry.amount(), Money::plus);
                }
            }
        }

        private void requireOpen() {
            if (!open) {
                throw new IllegalStateException("this Tx belongs to a transaction that has ended");
            }
        }

        @Override
        public PostResult post(LedgerTxn txn) {
            requireOpen();
            LedgerChecks.requireValid(txn.merchantId(), txn.entries());
            CauseKey key = CauseKey.of(txn);
            Optional<LedgerTxn> existing = lookup(key);
            if (existing.isPresent()) {
                return existing.get().sameEntriesAs(txn)
                        ? new PostResult.Duplicate(existing.get().id())
                        : new PostResult.LedgerConflict(existing.get(), txn);
            }
            pendingLedger.put(key, txn);
            pendingOrder.add(txn);
            return new PostResult.Posted(txn.id());
        }

        @Override
        public Optional<LedgerTxn> find(MerchantId merchantId, Cause cause) {
            requireOpen();
            return lookup(new CauseKey(merchantId, cause));
        }

        @Override
        public Money balance(MerchantId merchantId, AccountCode account) {
            requireOpen();
            Money total = balances.getOrDefault(new AccountKey(merchantId, account), Money.ZERO);
            for (LedgerTxn txn : pendingOrder) {
                for (Entry entry : txn.entries()) {
                    if (entry.merchantId().equals(merchantId) && entry.account() == account) {
                        total = total.plus(entry.amount());
                    }
                }
            }
            return total;
        }

        private Optional<LedgerTxn> lookup(CauseKey key) {
            LedgerTxn pending = pendingLedger.get(key);
            return pending != null ? Optional.of(pending) : Optional.ofNullable(ledger.get(key));
        }

        @Override
        public LedgerRepo ledger() {
            requireOpen();
            return this;
        }

        @Override
        public DedupeRepo dedupe() {
            return DEDUPE;
        }

        @Override
        public OrderRepo orders() {
            return ORDERS;
        }

        @Override
        public PaymentRepo payments() {
            return PAYMENTS;
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
        public PartitionRepo partitions() {
            return PARTITIONS;
        }

        @Override
        public AuditRepo audit() {
            return AUDIT;
        }
    }
}
