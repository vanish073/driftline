package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.Trace;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Driftline's resolver in the simulator (DESIGN §8): the real {@link Resolver} in a Store transaction, on this node's
 * own clock. It sleeps until the earliest review time and is woken by the matcher and the dispatcher, so it never
 * polls. A crash loses only its schedule; the next run reviews whatever is due.
 *
 * <p>Crash points: CP1 (decided, before commit: the transaction rolls back) and CP2 (committed, before any claim: the
 * dispatcher finds the commands later). Either is armed by a NODE_CRASH fault, or forced in dedicated seeds.
 */
public final class ResolverNode {

    public static final String NAME = "resolver";
    static final long WAKE_DELAY_MILLIS = 50;
    static final long RETRY_MILLIS = 1_000;
    static final int LIMIT = 20;

    private final Scheduler scheduler;
    private final SimClock clock;
    private final Store store;
    private final Trace trace;
    private final FaultInjector faults;
    private final Resolver resolver;
    private final ForcedCrash forced;
    private Runnable dispatcherWake = () -> { };
    private boolean down;
    private int incarnation = 1;
    private long scheduledAt = Long.MAX_VALUE;
    private Optional<CrashPoint> armed = Optional.empty();
    private long armedDowntimeMillis;
    private long decisions;

    public ResolverNode(Scheduler scheduler, SimClock clock, Store store, Trace trace, FaultInjector faults,
                        Resolver.Config config, Canaries canaries, ForcedCrash forced) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.store = store;
        this.trace = trace;
        this.faults = faults;
        this.forced = forced;
        this.resolver = new Resolver(config, canaries, point -> {
            if (point == CrashPoint.CP1 && crashesAt(CrashPoint.CP1)) {
                throw new SimCrash();
            }
        });
    }

    public void dispatcherWake(Runnable wake) {
        this.dispatcherWake = wake;
    }

    public SimClock clock() {
        return clock;
    }

    public long decisions() {
        return decisions;
    }

    /** New work may exist (a break raised or woken, a command result): run soon. */
    public void wake() {
        scheduleAfter(WAKE_DELAY_MILLIS);
    }

    private void scheduleAfter(long delayMillis) {
        if (down) {
            return;
        }
        long at = scheduler.now() + Math.max(WAKE_DELAY_MILLIS, delayMillis);
        if (scheduledAt > scheduler.now() && scheduledAt <= at) {
            return;   // an earlier run is already pending
        }
        scheduledAt = at;
        int inc = incarnation;
        scheduler.after(at - scheduler.now(), NAME, () -> {
            if (inc != incarnation || down || scheduledAt != at) {
                return;
            }
            scheduledAt = Long.MAX_VALUE;
            run();
        });
    }

    private void run() {
        Instant now = clock.now();
        Resolver.Outcome outcome;
        try {
            outcome = store.transact(NAME, tx -> resolver.step(tx, now, LIMIT));
        } catch (SimCrash crash) {
            crashNow(armedOrForcedDowntime(), "crashes at CP1 (decided, before commit), rolled back");
            return;
        } catch (StoreUnavailable | StoreConflict e) {
            faults.count("store.retries");
            scheduleAfter(RETRY_MILLIS);
            return;
        }
        if (!outcome.decisions().isEmpty()) {
            decisions += outcome.decisions().size();
            trace.line(scheduler.now(), NAME, "reviews " + String.join(" ", outcome.decisions()));
            if (crashesAt(CrashPoint.CP2)) {
                crashNow(armedOrForcedDowntime(), "crashes at CP2 (committed, before any claim)");
                return;
            }
            dispatcherWake.run();
        }
        if (outcome.decisions().size() == LIMIT) {
            scheduleAfter(WAKE_DELAY_MILLIS);
            return;
        }
        Optional<Instant> next;
        try {
            next = store.transact(NAME + "-next", tx -> tx.breaks().nextReviewAt());
        } catch (StoreUnavailable e) {
            scheduleAfter(RETRY_MILLIS);
            return;
        }
        next.ifPresent(at -> scheduleAfter(Duration.between(clock.now(), at).toMillis()));
    }

    // ---- faults ----

    private boolean crashesAt(CrashPoint point) {
        if (armed.filter(point::equals).isPresent()) {
            armed = Optional.empty();
            return true;
        }
        return forced.take(point, false);
    }

    private long armedOrForcedDowntime() {
        long d = armedDowntimeMillis > 0 ? armedDowntimeMillis : ForcedCrash.DOWNTIME_MILLIS;
        armedDowntimeMillis = 0;
        return d;
    }

    /** NODE_CRASH now. */
    public void crash(long downtimeMillis) {
        if (!down) {
            crashNow(downtimeMillis, "crashes");
        }
    }

    /** NODE_CRASH at a crash point: CP1 or CP2, chosen by the downtime's parity (seed-driven, no extra draw). */
    public void armCrash(long downtimeMillis) {
        if (!down) {
            armed = Optional.of(downtimeMillis % 2 == 0 ? CrashPoint.CP1 : CrashPoint.CP2);
            armedDowntimeMillis = downtimeMillis;
        }
    }

    private void crashNow(long downtimeMillis, String how) {
        down = true;
        armed = Optional.empty();
        int dead = ++incarnation;
        scheduledAt = Long.MAX_VALUE;
        faults.count("resolver.crashes");
        trace.line(scheduler.now(), NAME, how + ", down for " + downtimeMillis + " ms");
        scheduler.after(downtimeMillis, NAME, () -> restart(dead));
    }

    public void heal() {
        armed = Optional.empty();
        restart(incarnation);
    }

    private void restart(int deadIncarnation) {
        if (down && deadIncarnation == incarnation) {
            down = false;
            trace.line(scheduler.now(), NAME, "restarts");
            wake();
        }
    }
}
