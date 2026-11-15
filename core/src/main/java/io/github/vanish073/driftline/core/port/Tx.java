package io.github.vanish073.driftline.core.port;

/** The repositories visible inside one {@link Store} transaction (DESIGN §3.2). */
public interface Tx {

    DedupeRepo dedupe();

    OrderRepo orders();

    PaymentRepo payments();

    WatermarkRepo watermarks();

    TimerRepo timers();

    SettlementRepo settlement();

    BreakRepo breaks();

    OutboxRepo outbox();

    LedgerRepo ledger();

    PartitionRepo partitions();

    QuarantineRepo quarantine();

    AuditRepo audit();
}
