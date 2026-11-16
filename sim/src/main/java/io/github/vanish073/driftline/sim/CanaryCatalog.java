package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.sim.invariant.Invariants;
import java.util.List;

/**
 * The canaries the simulator can detect, and the invariants each one is expected to trip (DESIGN §11.10). A canary
 * caught by some other check counts as a miss: it would mean the run failed for an unrelated reason. All eleven are
 * active from Shift 8. C08 is expected to trip INV-INFERRED-HONEST, or first INV-MISSING-IS-REAL: a wrong inference
 * leaves the payment really in the line unmatched, so its MISSING_SETTLEMENT is false (D57). C03 shows up as a lost
 * refund (INV-CUSTOMER-WHOLE) or as a refund the gateway made but nobody booked (INV-BOOKS-MATCH-WORLD); C05 and C06
 * gain BOOKS-MATCH-WORLD and CUSTOMER-WHOLE, as D37 planned.
 */
public final class CanaryCatalog {

    /** Each active canary must be caught within this many {@code pr} seeds (Shift 4 Prove). */
    public static final int SEED_BUDGET = 500;

    private CanaryCatalog() {
    }

    /** The active canaries, in declaration order. */
    public static List<Canary> active() {
        return List.of(Canary.values());
    }

    public static List<String> expected(Canary canary) {
        return switch (canary) {
            case DEDUPE_OFF -> List.of(Invariants.EVENT_ONCE, Invariants.LEDGER_ONCE);
            case NO_REFUND_KEY -> List.of(Invariants.REFUND_ONCE);
            case MARK_BEFORE_SEND -> List.of(Invariants.CUSTOMER_WHOLE, Invariants.BOOKS_MATCH_WORLD);
            case SKIP_TTL_LOOKUP -> List.of(Invariants.REFUND_ONCE);
            case LEDGER_UNIQUE_OFF -> List.of(Invariants.LEDGER_ONCE, Invariants.BOOKS_MATCH_WORLD);
            case SUCCESS_TO_FAILED -> List.of(Invariants.TRANSITIONS_LEGAL, Invariants.ONE_OUTCOME,
                    Invariants.CUSTOMER_WHOLE);
            case PROCESSING_TIME_WATERMARK -> List.of(Invariants.MISSING_IS_REAL);
            case FIRST_SOLUTION_WINS -> List.of(Invariants.INFERRED_HONEST, Invariants.MISSING_IS_REAL);
            case NO_FENCING -> List.of(Invariants.FENCED, Invariants.LEDGER_ONCE);
            case IGNORE_ESCALATE_BY -> List.of(Invariants.DEADLINE);
            case BREAK_SCOPED_REFUND_KEY -> List.of(Invariants.REFUND_ONCE);
        };
    }
}
