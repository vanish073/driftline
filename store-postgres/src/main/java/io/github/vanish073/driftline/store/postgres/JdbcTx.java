package io.github.vanish073.driftline.store.postgres;

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
import io.github.vanish073.driftline.core.port.TimerRepo;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.core.port.WatermarkRepo;
import java.sql.Connection;

/** One database transaction seen through the core's repositories. Unusable once its transaction has ended. */
final class JdbcTx implements Tx {

    private static final WatermarkRepo WATERMARKS = new WatermarkRepo() { };
    private static final TimerRepo TIMERS = new TimerRepo() { };
    private static final SettlementRepo SETTLEMENT = new SettlementRepo() { };
    private static final BreakRepo BREAKS = new BreakRepo() { };
    private static final OutboxRepo OUTBOX = new OutboxRepo() { };
    private static final AuditRepo AUDIT = new AuditRepo() { };

    private final Connection connection;
    private final JdbcLedgerRepo ledger;
    private final DedupeRepo dedupeRepo = new JdbcStateRepos.Dedupe(this);
    private final PaymentRepo paymentsRepo = new JdbcStateRepos.Payments(this);
    private final OrderRepo ordersRepo = new JdbcStateRepos.Orders(this);
    private final PartitionRepo partitionsRepo = new JdbcStateRepos.Partitions(this);
    private final QuarantineRepo quarantineRepo = new JdbcStateRepos.Quarantine(this);
    private boolean open = true;

    JdbcTx(Connection connection, BalanceStrategy strategy) {
        this.connection = connection;
        this.ledger = new JdbcLedgerRepo(this, strategy);
    }

    Connection connection() {
        if (!open) {
            throw new IllegalStateException("this Tx belongs to a transaction that has ended");
        }
        return connection;
    }

    void close() {
        open = false;
    }

    @Override
    public LedgerRepo ledger() {
        connection();
        return ledger;
    }

    @Override
    public DedupeRepo dedupe() {
        connection();
        return dedupeRepo;
    }

    @Override
    public OrderRepo orders() {
        connection();
        return ordersRepo;
    }

    @Override
    public PaymentRepo payments() {
        connection();
        return paymentsRepo;
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
        connection();
        return partitionsRepo;
    }

    @Override
    public QuarantineRepo quarantine() {
        connection();
        return quarantineRepo;
    }

    @Override
    public AuditRepo audit() {
        return AUDIT;
    }
}
