package io.github.vanish073.driftline.app.dispatcher;

import io.github.vanish073.driftline.app.port.HttpNetwork;
import io.github.vanish073.driftline.core.dispatcher.Dispatcher;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.Network;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.wire.call.CallWire;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The dispatcher role (DESIGN §9.2): the real {@link Dispatcher}, the same code the simulator's dispatcher node runs, on
 * one platform thread. Each round it claims due outbox commands in a Store transaction (leased to this process, {@code
 * FOR UPDATE SKIP LOCKED}), sends the calls through the production {@link HttpNetwork}, and records each reply in
 * another transaction. Replies arrive on the HTTP client's threads and are queued for this thread, so every Store
 * write happens here. Between rounds it waits for a reply, a wake, or the earliest next attempt, but never longer than
 * {@code pollInterval}, because the resolver may run in another process.
 *
 * <p>Only an answer is a result ({@link CallWire#reply}): a transport error, a 5xx or an unreadable body records
 * nothing, so the lease runs out and the call is retried with the same key. A reply is a fact and is recorded whatever
 * attempt it answers.
 *
 * <p>Crash points: CP3 (claim committed, before send) and CP4 (sent, before the reply is recorded) are here; CP5 is
 * inside {@link Dispatcher#onResult} (reply received, before the record commits). A crash point that throws stops this
 * thread, and replies still in flight are lost with it, as they would be with the process.
 */
public final class DispatcherDriver implements SmartLifecycle {

    /**
     * What the driver did with each call (§13, D78). Called on the dispatcher thread; must not throw.
     */
    public interface Listener {
        Listener NONE = new Listener() { };

        /** Hands one call to the network by running {@code send}; a trace span may be current while it runs. */
        default void send(OutboundCall call, Runnable send) {
            send.run();
        }

        /** How an attempt ended: {@code answered}, {@code no_reply}, {@code no_result} or {@code lost} (see below). */
        default void attempt(OutboundCall call, String result) {
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(DispatcherDriver.class);
    static final Duration MIN_SLEEP = Duration.ofMillis(50);

    private final Store store;
    private final Dispatcher dispatcher;
    private final Clock clock;
    private final CrashPoints crashPoints;
    private final Network network;
    private final String owner;
    private final Duration pollInterval;
    private final Listener listener;
    /** Replies, and {@code Optional.empty()} as a wake. */
    private final LinkedBlockingQueue<Optional<HttpNetwork.CallOutcome>> inbox = new LinkedBlockingQueue<>();

    private volatile boolean running;
    private volatile Thread thread;
    private volatile Throwable failure;

    /** @param network builds the network that delivers each call's outcome to the consumer it is given */
    public DispatcherDriver(Store store, Dispatcher dispatcher, Clock clock, CrashPoints crashPoints,
                            Function<Consumer<HttpNetwork.CallOutcome>, Network> network, String owner,
                            Duration pollInterval) {
        this(store, dispatcher, clock, crashPoints, network, owner, pollInterval, Listener.NONE);
    }

    public DispatcherDriver(Store store, Dispatcher dispatcher, Clock clock, CrashPoints crashPoints,
                            Function<Consumer<HttpNetwork.CallOutcome>, Network> network, String owner,
                            Duration pollInterval, Listener listener) {
        this.listener = Objects.requireNonNull(listener, "listener");
        this.store = Objects.requireNonNull(store, "store");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.crashPoints = Objects.requireNonNull(crashPoints, "crashPoints");
        this.network = network.apply(outcome -> {
            if (running) {
                inbox.add(Optional.of(outcome));
            }
        });
        this.owner = Objects.requireNonNull(owner, "owner");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            failure = null;
            thread = Thread.ofPlatform().name("dispatcher").start(this::run);
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

    public String owner() {
        return owner;
    }

    /** New commands may exist (the resolver in this process decided something): claim soon. */
    public void wake() {
        inbox.add(Optional.empty());
    }

    private void run() {
        try {
            while (running) {
                claimAndSend();
                if (!awaitReplies()) {
                    return;
                }
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            running = false;
            inbox.clear();
            LOG.error("dispatcher stopped; the process must restart, and its leases run out and are claimed again", e);
        }
    }

    /** Claims due commands and sends their calls; returns how many were sent. Public for tests. */
    public int claimAndSend() {
        List<OutboundCall> calls;
        try {
            Instant now = clock.now();
            calls = store.transact("dispatcher-claim", tx -> dispatcher.claim(tx, now, owner));
        } catch (StoreUnavailable | StoreConflict e) {
            LOG.warn("dispatcher claim will be retried: {}", e.getMessage());
            return 0;
        }
        if (calls.isEmpty()) {
            return 0;
        }
        crashPoints.reached(CrashPoint.CP3);
        for (OutboundCall call : calls) {
            send(call);
        }
        return calls.size();
    }

    private void send(OutboundCall call) {
        LOG.info("dispatcher sends {} {} to {}", call.callId(), call.request().getClass().getSimpleName(), call.target());
        listener.send(call, () -> network.send(call));
        crashPoints.reached(CrashPoint.CP4);
    }

    /**
     * Waits for replies until a reply was recorded, a wake arrives, or the next attempt is due; false if interrupted.
     */
    private boolean awaitReplies() {
        Instant deadline = clock.now().plus(sleepTime());
        while (running) {
            long waitMillis = Math.max(0, Duration.between(clock.now(), deadline).toMillis());
            Optional<HttpNetwork.CallOutcome> next;
            try {
                next = inbox.poll(waitMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
            if (next == null || next.isEmpty()) {
                return true;   // due, or woken
            }
            if (record(next.get())) {
                return true;   // a result may have enqueued an observation or a follow-up: claim again
            }
        }
        return true;
    }

    /** Records one reply if it is a result (§9.2 step 3); returns whether it was. Public for tests. */
    public boolean record(HttpNetwork.CallOutcome outcome) {
        OutboundCall call = outcome.call();
        if (outcome.status().isEmpty()) {
            LOG.info("dispatcher: no reply to {} ({}); retried after its lease", call.callId(),
                    outcome.error().orElse("no status"));
            listener.attempt(call, "no_reply");
            return false;
        }
        Optional<TreeMap<String, String>> facts = CallWire.reply(call.request(), outcome.status().get(), outcome.body());
        if (facts.isEmpty()) {
            LOG.info("dispatcher: {} answered {} with no result; retried after its lease", call.callId(),
                    outcome.status().get());
            listener.attempt(call, "no_result");
            return false;
        }
        List<OutboundCall> then;
        try {
            Instant now = clock.now();
            then = store.transact("dispatcher-result", tx -> dispatcher.onResult(tx, call, facts.get(), now, owner));
        } catch (StoreUnavailable | StoreConflict e) {
            LOG.warn("dispatcher: the result of {} is lost ({}); the call is retried after its lease", call.callId(),
                    e.getMessage());
            listener.attempt(call, "lost");
            return false;
        }
        LOG.info("dispatcher records {} {}", call.callId(), facts.get());
        listener.attempt(call, "answered");
        for (OutboundCall c : then) {
            send(c);
        }
        return true;
    }

    private Duration sleepTime() {
        Optional<Instant> next;
        try {
            next = store.transact("dispatcher-next", tx -> tx.outbox().nextAttemptAt());
        } catch (StoreUnavailable | StoreConflict e) {
            return pollInterval;
        }
        Duration wait = next.map(at -> Duration.between(clock.now(), at)).orElse(pollInterval);
        return wait.compareTo(MIN_SLEEP) < 0 ? MIN_SLEEP : wait.compareTo(pollInterval) > 0 ? pollInterval : wait;
    }
}
