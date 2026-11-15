package io.github.vanish073.driftline.sim;

import io.github.vanish073.driftline.core.hooks.Canary;
import io.github.vanish073.driftline.sim.invariant.Invariants;
import java.util.List;

/**
 * The canaries the simulator can detect so far, and the invariants each one is expected to trip (DESIGN §11.10).
 * A canary caught by some other check counts as a miss: it would mean the run failed for an unrelated reason.
 * The others arrive with the code they plant a bug in (C02–C04, C10, C11 with the resolver and dispatcher; C08 with the
 * solver).
 */
public final class CanaryCatalog {

    /** Each active canary must be caught within this many {@code pr} seeds (Shift 4 Prove). */
    public static final int SEED_BUDGET = 500;

    private CanaryCatalog() {
    }

    /** The active canaries, in declaration order. */
    public static List<Canary> active() {
        return List.of(Canary.DEDUPE_OFF, Canary.LEDGER_UNIQUE_OFF, Canary.SUCCESS_TO_FAILED,
                Canary.PROCESSING_TIME_WATERMARK, Canary.NO_FENCING);
    }

    public static List<String> expected(Canary canary) {
        return switch (canary) {
            case DEDUPE_OFF -> List.of(Invariants.EVENT_ONCE, Invariants.LEDGER_ONCE);
            case LEDGER_UNIQUE_OFF -> List.of(Invariants.LEDGER_ONCE);
            case SUCCESS_TO_FAILED -> List.of(Invariants.TRANSITIONS_LEGAL, Invariants.ONE_OUTCOME);
            case PROCESSING_TIME_WATERMARK -> List.of(Invariants.MISSING_IS_REAL);
            case NO_FENCING -> List.of(Invariants.FENCED, Invariants.LEDGER_ONCE);
            default -> throw new IllegalArgumentException(canary.code() + " is not active yet");
        };
    }
}
