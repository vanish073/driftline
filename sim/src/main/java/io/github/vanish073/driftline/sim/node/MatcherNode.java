package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Trace;
import java.util.List;
import java.util.function.Consumer;

/**
 * The partition driver of one matcher in the simulator (DESIGN §3.1): it reads a batch from {@code SimLog} at the
 * offset stored in the Store and hands it to the real core {@link Matcher}, in exactly one Store transaction per step.
 */
public final class MatcherNode {

    public static final int MAX_BATCH = 100;
    static final long POLL_DELAY_MILLIS = 50;

    private final int partition;
    private final Store store;
    private final SimLog log;
    private final Scheduler scheduler;
    private final Trace trace;
    private final Consumer<String> anomalies;
    private final Matcher matcher = new Matcher();
    private final String node;
    private boolean scheduled;
    private int incarnation = 1;

    public MatcherNode(int partition, Store store, SimLog log, Scheduler scheduler, Trace trace, Consumer<String> anomalies) {
        this.partition = partition;
        this.store = store;
        this.log = log;
        this.scheduler = scheduler;
        this.trace = trace;
        this.anomalies = anomalies;
        this.node = "matcher-" + partition;
    }

    /** Asks for a poll soon; repeated wakes before the poll collapse into one. */
    public void wake() {
        if (!scheduled) {
            scheduled = true;
            int forIncarnation = incarnation;
            scheduler.after(POLL_DELAY_MILLIS, node, () -> poll(forIncarnation));
        }
    }

    private void poll(int forIncarnation) {
        if (forIncarnation != incarnation) {
            return;   // scheduled by an incarnation that has since crashed (Shift 4)
        }
        scheduled = false;
        // Exactly one Store transaction per step (DESIGN §11.1): read the offset, process, and learn whether more waits.
        Polled polled = store.transact(node + "-batch", tx -> {
            List<LogRecord> records = log.read(partition, tx.partitions().nextOffset(partition), MAX_BATCH);
            BatchOutcome outcome = records.isEmpty() ? null : matcher.process(new PartitionBatch(partition, records), tx);
            return new Polled(outcome, tx.partitions().nextOffset(partition));
        });
        if (polled.outcome() != null) {
            BatchOutcome outcome = polled.outcome();
            trace.line(scheduler.now(), node, "batch applied=" + outcome.applied() + " dup=" + outcome.duplicates()
                    + " reused=" + outcome.keyReused() + " later=" + outcome.notYetHandled() + " posted=" + outcome.postings());
            if (outcome.keyReused() > 0) {
                anomalies.accept("IDEMPOTENCY_CONFLICT: " + outcome.keyReused() + " event key(s) reused with a different body");
            }
            for (var conflict : outcome.ledgerConflicts()) {
                anomalies.accept("LEDGER_CONFLICT: " + conflict.existing() + " vs " + conflict.attempted());
            }
        }
        if (polled.nextOffset() < log.endOffset(partition)) {
            wake();
        }
    }

    private record Polled(BatchOutcome outcome, long nextOffset) {
    }

    public int partition() {
        return partition;
    }

    public int incarnation() {
        return incarnation;
    }
}
