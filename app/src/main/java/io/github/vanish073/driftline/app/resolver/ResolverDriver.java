package io.github.vanish073.driftline.app.resolver;

import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.resolver.Resolver;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The resolver role (DESIGN §8): the real {@link Resolver}, the same code the simulator's resolver node runs, in one
 * Store transaction per step on its own platform thread. It reviews whatever is due, then sleeps until the earliest
 * review time, but never longer than {@code pollInterval}, because the matcher raising a break may run in another
 * process. A step takes its due breaks {@code FOR UPDATE SKIP LOCKED}, so two resolvers never decide one break at the
 * same time; the simulator and the Compose stack run one resolver.
 *
 * <p>Crash points: CP1 is inside {@link Resolver#step} (decided, before commit: the transaction rolls back); CP2 is
 * here, after a commit that decided something and before the dispatcher could claim it. A crash point that throws
 * stops this thread, as a process crash would; the health indicator then reports DOWN and the process is restarted.
 */
public final class ResolverDriver implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ResolverDriver.class);
    static final Duration MIN_SLEEP = Duration.ofMillis(50);

    private final Store store;
    private final Resolver resolver;
    private final Clock clock;
    private final CrashPoints crashPoints;
    private final Duration pollInterval;
    private final int limit;
    private final Runnable afterDecisions;

    private volatile boolean running;
    private volatile Thread thread;
    private volatile Throwable failure;

    /** @param afterDecisions run after a step that decided something (wakes a dispatcher in the same process) */
    public ResolverDriver(Store store, Resolver resolver, Clock clock, CrashPoints crashPoints, Duration pollInterval,
                          int limit, Runnable afterDecisions) {
        this.store = Objects.requireNonNull(store, "store");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.crashPoints = Objects.requireNonNull(crashPoints, "crashPoints");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.limit = limit;
        this.afterDecisions = Objects.requireNonNull(afterDecisions, "afterDecisions");
        if (limit < 1) {
            throw new IllegalArgumentException("review at least one break per step");
        }
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            failure = null;
            thread = Thread.ofPlatform().name("resolver").start(this::run);
        }
    }

    @Override
    public void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        Thread t = thread;
        return running && t != null && t.isAlive();
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure);
    }

    private void run() {
        try {
            while (running) {
                int decided = stepOnce();
                if (decided < limit) {
                    try {
                        Thread.sleep(sleepTime());
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            running = false;
            LOG.error("resolver stopped; the process must restart, and the next step reviews whatever is due", e);
        }
    }

    /** One resolver step; returns how many breaks it decided. Public for tests. */
    public int stepOnce() {
        Resolver.Outcome outcome;
        try {
            Instant now = clock.now();
            outcome = store.transact("resolver", tx -> resolver.step(tx, now, limit));
        } catch (StoreUnavailable | StoreConflict e) {
            LOG.warn("resolver step will be retried: {}", e.getMessage());
            return 0;
        }
        int decided = outcome.decisions().size();
        if (decided > 0) {
            LOG.info("resolver decided {}", String.join(" ", outcome.decisions()));
            crashPoints.reached(CrashPoint.CP2);
            afterDecisions.run();
        }
        return decided;
    }

    private Duration sleepTime() {
        Optional<Instant> next;
        try {
            next = store.transact("resolver-next", tx -> tx.breaks().nextReviewAt());
        } catch (StoreUnavailable | StoreConflict e) {
            return pollInterval;
        }
        Duration wait = next.map(at -> Duration.between(clock.now(), at)).orElse(pollInterval);
        return wait.compareTo(MIN_SLEEP) < 0 ? MIN_SLEEP : wait.compareTo(pollInterval) > 0 ? pollInterval : wait;
    }
}
