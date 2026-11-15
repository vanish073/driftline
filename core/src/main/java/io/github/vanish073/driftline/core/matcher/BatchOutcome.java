package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.ledger.PostResult;
import java.util.List;

/**
 * What one batch did. {@code ledgerConflicts} must always be empty: a ledger conflict means two different postings
 * for one economic event, which is a bug, and the simulator fails the seed on it (DESIGN §7.4).
 *
 * @param alreadyProcessed records below the stored offset, skipped (a replay after a crash or a rebalance)
 * @param duplicates       records whose event key was already applied (dedupe, D3)
 * @param keyReused        records whose event key was applied with a different payload (IDEMPOTENCY_CONFLICT);
 *                         each one is quarantined
 * @param notYetHandled    refund and settlement events, which the matcher handles from Shift 5
 */
public record BatchOutcome(int applied, int alreadyProcessed, int duplicates, int keyReused, int notYetHandled,
                           int postings, List<PostResult.LedgerConflict> ledgerConflicts) {

    public BatchOutcome {
        ledgerConflicts = List.copyOf(ledgerConflicts);
    }
}
