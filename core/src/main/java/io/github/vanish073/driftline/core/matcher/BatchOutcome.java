package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.ledger.PostResult;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * What one batch or tick did. {@code ledgerConflicts} must always be empty: a ledger conflict means two different
 * postings for one economic event, which is a bug, and the simulator fails the seed on it (DESIGN §7.4).
 *
 * <p>After the transaction commits, the driver hands the outcome back to {@link Matcher#committed} so the keyed-state
 * cache learns the batch's writes, and schedules a tick at {@code nextWakeAt}.
 *
 * @param alreadyProcessed records below the stored offset, skipped (a replay after a crash or a rebalance)
 * @param duplicates       records whose event key was already applied (dedupe, D3)
 * @param keyReused        records whose event key was applied with a different payload (IDEMPOTENCY_CONFLICT);
 *                         each one is quarantined
 * @param notYetHandled    refund events, which the matcher handles from Shift 8
 * @param raised           breaks, anomalies and alerts first raised by this batch
 * @param timersFired      timers that fired in this batch
 * @param lateEvents       events that arrived behind their stream watermark (processed normally, §5.4)
 * @param nextWakeAt       the processing time at which a tick may fire timers that are not due yet, if any
 */
public record BatchOutcome(int applied, int alreadyProcessed, int duplicates, int keyReused, int notYetHandled,
                           int postings, List<PostResult.LedgerConflict> ledgerConflicts, List<Break> raised,
                           int timersFired, int lateEvents, Optional<Instant> nextWakeAt, KeyedState.Delta delta) {

    public BatchOutcome {
        ledgerConflicts = List.copyOf(ledgerConflicts);
        raised = List.copyOf(raised);
        Objects.requireNonNull(nextWakeAt, "nextWakeAt");
        Objects.requireNonNull(delta, "delta");
    }
}
