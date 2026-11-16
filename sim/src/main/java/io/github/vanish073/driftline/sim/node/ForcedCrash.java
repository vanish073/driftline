package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.hooks.CrashPoint;
import java.util.Optional;

/**
 * A crash point forced on purpose in a dedicated seed (Shift 8 Prove: "each crash point is forced on purpose in
 * dedicated seeds"): the node that reaches {@code point} crashes the first {@code times} times it does, each time for
 * {@link #DOWNTIME_MILLIS}. Mutable, one per run.
 */
public final class ForcedCrash {

    public static final long DOWNTIME_MILLIS = 5_000L;

    private final Optional<CrashPoint> point;
    private int remaining;
    private int taken;

    /** No forced crash. */
    public static ForcedCrash none() {
        return new ForcedCrash(Optional.empty(), 0);
    }

    public ForcedCrash(Optional<CrashPoint> point, int times) {
        this.point = point;
        this.remaining = times;
    }

    /**
     * Whether to crash now at {@code reached}; counts the crash if so. The dispatcher's points (CP3-CP5) strike only
     * a REFUND call ({@code refund}), since their required outcome is about one refund (DESIGN §9.4).
     */
    boolean take(CrashPoint reached, boolean refund) {
        boolean dispatcherPoint = reached.ordinal() >= CrashPoint.CP3.ordinal();
        if (point.filter(reached::equals).isEmpty() || remaining == 0 || dispatcherPoint && !refund) {
            return false;
        }
        remaining--;
        taken++;
        return true;
    }

    public Optional<CrashPoint> point() {
        return point;
    }

    /** How many forced crashes happened. */
    public int taken() {
        return taken;
    }
}
