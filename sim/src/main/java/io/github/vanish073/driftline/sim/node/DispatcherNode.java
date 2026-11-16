package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.dispatcher.Dispatcher;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.Trace;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Driftline's dispatcher in the simulator (DESIGN §9.2): the real {@link Dispatcher} claims due outbox commands in a
 * Store transaction, the node sends the calls through a {@link Router} over the simulated network, and records each
 * reply in another transaction. It sleeps until the earliest next attempt and is woken by the resolver and the
 * matcher. Its lease owner is {@code dispatcher.<incarnation>}, so after a crash the old leases are no longer its own,
 * and a reply that reaches a dead incarnation is lost.
 *
 * <p>Crash points: CP3 (claim committed, before send), CP4 (sent, and the target applied it, but the response is
 * lost with this incarnation) and CP5 (result received, before the record commits). Each is armed by a NODE_CRASH
 * fault, or forced in dedicated seeds.
 */
public final class DispatcherNode {

    public static final String NAME = "dispatcher";
    static final long WAKE_DELAY_MILLIS = 50;
    static final long RETRY_MILLIS = 1_000;

    /** Delivers a call to its target and the target's answer back; {@code reply} may never be called. */
    public interface Router {
        void send(OutboundCall call, Consumer<Map<String, String>> reply);
    }

    private final Scheduler scheduler;
    private final SimClock clock;
    private final Store store;
    private final Trace trace;
    private final FaultInjector faults;
    private final Dispatcher dispatcher;
    private final Router router;
    private final ForcedCrash forced;
    private Runnable resolverWake = () -> { };
    private boolean down;
    private int incarnation = 1;
    private long scheduledAt = Long.MAX_VALUE;
    private Optional<CrashPoint> armed = Optional.empty();
    private long armedDowntimeMillis;
    private long sent;
    private long results;
    /** Whether the result being recorded is a refund's (for a forced CP5). */
    private boolean recording;

    public DispatcherNode(Scheduler scheduler, SimClock clock, Store store, Trace trace, FaultInjector faults,
                          Dispatcher.Config config, Canaries canaries, Router router, ForcedCrash forced) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.store = store;
        this.trace = trace;
        this.faults = faults;
        this.router = router;
        this.forced = forced;
        this.dispatcher = new Dispatcher(config, canaries, point -> {
            if (point == CrashPoint.CP5 && crashesAt(CrashPoint.CP5, recording)) {
                throw new SimCrash();
            }
        });
    }

    public void resolverWake(Runnable wake) {
        this.resolverWake = wake;
    }

    public SimClock clock() {
        return clock;
    }

    public long sent() {
        return sent;
    }

    public long results() {
        return results;
    }

    private String owner() {
        return NAME + "." + incarnation;
    }

    /** New commands may exist: run soon. */
    public void wake() {
        scheduleAfter(WAKE_DELAY_MILLIS);
    }

    private void scheduleAfter(long delayMillis) {
        if (down) {
            return;
        }
        long at = scheduler.now() + Math.max(WAKE_DELAY_MILLIS, delayMillis);
        if (scheduledAt > scheduler.now() && scheduledAt <= at) {
            return;
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
        List<OutboundCall> calls;
        try {
            calls = store.transact(NAME + "-claim", tx -> dispatcher.claim(tx, now, owner()));
        } catch (StoreUnavailable | StoreConflict e) {
            faults.count("store.retries");
            scheduleAfter(RETRY_MILLIS);
            return;
        }
        if (!calls.isEmpty() && crashesAt(CrashPoint.CP3, calls.stream().anyMatch(DispatcherNode::isRefund))) {
            crashNow(armedOrForcedDowntime(), "crashes at CP3 (claimed " + calls.size() + ", before send)");
            return;
        }
        for (OutboundCall call : calls) {
            send(call);
            if (down) {
                return;   // CP4 struck after this send
            }
        }
        scheduleNext();
    }

    private void scheduleNext() {
        Optional<Instant> next;
        try {
            next = store.transact(NAME + "-next", tx -> tx.outbox().nextAttemptAt());
        } catch (StoreUnavailable e) {
            scheduleAfter(RETRY_MILLIS);
            return;
        }
        next.ifPresent(at -> scheduleAfter(Duration.between(clock.now(), at).toMillis()));
    }

    private void send(OutboundCall call) {
        int inc = incarnation;
        sent++;
        trace.line(scheduler.now(), NAME, "sends " + call.callId() + " " + call.request().getClass().getSimpleName()
                + " to " + call.target());
        router.send(call, facts -> onReply(inc, call, facts));
        if (crashesAt(CrashPoint.CP4, isRefund(call))) {
            crashNow(armedOrForcedDowntime(), "crashes at CP4 (sent " + call.callId() + "; its response is lost)");
        }
    }

    private void onReply(int inc, OutboundCall call, Map<String, String> facts) {
        if (inc != incarnation || down) {
            trace.line(scheduler.now(), NAME, "reply to " + call.callId() + " lost with incarnation " + inc);
            return;
        }
        Instant now = clock.now();
        List<OutboundCall> then;
        recording = isRefund(call);
        try {
            then = store.transact(NAME + "-result", tx -> dispatcher.onResult(tx, call, facts, now, owner()));
        } catch (SimCrash crash) {
            crashNow(armedOrForcedDowntime(), "crashes at CP5 (result of " + call.callId() + " received, before commit)");
            return;
        } catch (StoreUnavailable | StoreConflict e) {
            faults.count("store.retries");   // the result is lost; the lease runs out and the call is retried
            return;
        }
        results++;
        trace.line(scheduler.now(), NAME, "records " + call.callId() + " " + facts);
        for (OutboundCall c : then) {
            send(c);
            if (down) {
                return;
            }
        }
        resolverWake.run();
        wake();   // the result may have enqueued an observation
    }

    // ---- faults ----

    private boolean crashesAt(CrashPoint point, boolean refund) {
        if (armed.filter(point::equals).isPresent()) {
            armed = Optional.empty();
            return true;
        }
        return forced.take(point, refund);
    }

    private static boolean isRefund(OutboundCall call) {
        return call.request() instanceof CallRequest.Refund;
    }

    private long armedOrForcedDowntime() {
        long d = armedDowntimeMillis > 0 ? armedDowntimeMillis : ForcedCrash.DOWNTIME_MILLIS;
        armedDowntimeMillis = 0;
        return d;
    }

    public void crash(long downtimeMillis) {
        if (!down) {
            crashNow(downtimeMillis, "crashes");
        }
    }

    /** NODE_CRASH at a crash point: CP3, CP4 or CP5, chosen by the downtime (seed-driven, no extra draw). */
    public void armCrash(long downtimeMillis) {
        if (!down) {
            armed = Optional.of(switch ((int) (downtimeMillis % 3)) {
                case 0 -> CrashPoint.CP3;
                case 1 -> CrashPoint.CP4;
                default -> CrashPoint.CP5;
            });
            armedDowntimeMillis = downtimeMillis;
        }
    }

    private void crashNow(long downtimeMillis, String how) {
        down = true;
        armed = Optional.empty();
        int dead = ++incarnation;
        scheduledAt = Long.MAX_VALUE;
        faults.count("dispatcher.crashes");
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
            trace.line(scheduler.now(), NAME, "restarts as " + owner());
            wake();
        }
    }
}
