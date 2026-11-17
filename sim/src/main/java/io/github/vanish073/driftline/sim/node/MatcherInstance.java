package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.KeyedState;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MatcherConfig;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.port.PartitionRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Trace;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * One matcher process in the simulator: the partition driver of DESIGN §3.1 around the real core {@link Matcher}.
 * Like a Kafka consumer it holds, per assigned partition, an epoch (from {@code acquire}, §5.1) and an in-memory
 * position (seeked to the stored offset on assignment), and fetches from {@code SimLog} at that position. Each step
 * runs at most one Store transaction (§11.1).
 *
 * <p>Faults: a crash loses every in-memory thing (position, epoch, scheduled work; the incarnation number drops
 * stale callbacks) and restarts as a new group member. A crash or pause may be <em>armed</em> to strike at the next
 * batch: a crash then dies inside the batch transaction before commit (CP6); a pause freezes after the fetch and
 * before the transaction, holding the batch. If the pause outlasts the session timeout, the partition moves and the
 * instance wakes as a zombie with a stale epoch; its held batch is then fenced by the Store (INV-FENCED, CP7).
 *
 * <p>Shift 5: each batch carries this node's processing clock; after each committed transaction the outcome goes back
 * to the matcher so its keyed-state cache learns the writes (§5.2), and a tick is scheduled at the outcome's
 * {@code nextWakeAt}, so idle watermarks advance and timers fire with no new records (§5.4, §5.5). The cache is
 * dropped whenever ownership changes or the node crashes, and warmed after every acquire.
 */
public final class MatcherInstance {

    public static final int MAX_BATCH = 100;
    static final long POLL_DELAY_MILLIS = 50;
    static final long STORE_RETRY_MILLIS = 1_000;
    /**
     * D50, BUG-003: a fenced partition that the coordinator still assigns to this member after three session timeouts
     * is acquired again, as the production driver's {@code reacquireAfter} does. Without it, a zombie's epoch bump on a
     * partition it no longer owns left the rightful owner fenced, and the partition stalled forever.
     */
    static final long REACQUIRE_AFTER_MILLIS = 3 * Coordinator.SESSION_TIMEOUT_MILLIS;
    /** The shortest wait before a tick, so a timer that is not due yet never spins. */
    static final long MIN_TICK_DELAY_MILLIS = 1_000;

    private final String name;
    private final Store store;
    private final SimLog log;
    private final Scheduler scheduler;
    private final Trace trace;
    private final Coordinator coordinator;
    private final FaultInjector faults;
    private final Consumer<String> anomalies;
    /** Runs after every committed batch or tick: new breaks, woken breaks or probes may need the resolver or dispatcher. */
    private Runnable afterCommit = () -> { };
    private final Matcher matcher;
    private final SimClock clock;
    private final TreeMap<Integer, PartitionConsumer> consumers = new TreeMap<>();
    private final List<Runnable> deferred = new ArrayList<>();
    private int incarnation = 1;
    private int memberEpoch = 1;
    private boolean down;
    private boolean paused;
    private long pauseSeq;
    private long armedPauseMillis;
    private long armedCrashDowntimeMillis;

    public MatcherInstance(String name, Store store, SimLog log, Scheduler scheduler, Trace trace, Coordinator coordinator,
                           FaultInjector faults, MerchantDirectory merchants, MatcherConfig config, Canaries canaries,
                           Consumer<String> anomalies) {
        this.name = name;
        this.store = store;
        this.log = log;
        this.scheduler = scheduler;
        this.trace = trace;
        this.coordinator = coordinator;
        this.faults = faults;
        this.anomalies = anomalies;
        this.matcher = new Matcher(merchants, config, canaries);
        this.clock = new SimClock(scheduler);
        coordinator.register(this);
    }

    public String name() {
        return name;
    }

    public void afterCommit(Runnable hook) {
        this.afterCommit = hook;
    }

    /** The group member id: a new one after every restart or rejoin, as Kafka issues. */
    public String memberId() {
        return name + "." + memberEpoch;
    }

    public SimClock clock() {
        return clock;
    }

    public boolean isDown() {
        return down;
    }

    public boolean isPaused() {
        return paused;
    }

    public long cacheHits() {
        return matcher.keyedState().hits();
    }

    public long cacheMisses() {
        return matcher.keyedState().misses();
    }

    /** Runs {@code action} after {@code delay} in this incarnation; held while paused, dropped after a crash. */
    private void schedule(long delayMillis, Runnable action) {
        int forIncarnation = incarnation;
        scheduler.after(delayMillis, name, () -> {
            if (forIncarnation != incarnation || down) {
                return;
            }
            if (paused) {
                deferred.add(action);
                return;
            }
            action.run();
        });
    }

    void assign(int partition) {
        schedule(0, () -> {
            PartitionConsumer c = new PartitionConsumer(partition);
            consumers.put(partition, c);
            c.acquire();
        });
    }

    void wake(int partition) {
        PartitionConsumer c = consumers.get(partition);
        if (c != null) {
            c.wake();
        }
    }

    // ---- faults -----------------------------------------------------------------------------------------------

    /** NODE_CRASH now (not mid-batch). */
    public void crash(long downtimeMillis) {
        if (!down) {
            crashNow(downtimeMillis, "crashes");
        }
    }

    /** NODE_CRASH at the next batch, inside its transaction, before commit. */
    public void armCrash(long downtimeMillis) {
        if (!down) {
            armedCrashDowntimeMillis = downtimeMillis;
        }
    }

    /** NODE_PAUSE at the next batch, between its fetch and its transaction. */
    public void armPause(long durationMillis) {
        if (!down && !paused) {
            armedPauseMillis = durationMillis;
        }
    }

    /** HEAL: restart if down, resume if paused, disarm. */
    public void heal() {
        armedPauseMillis = 0;
        armedCrashDowntimeMillis = 0;
        if (down) {
            restart(incarnation);
        } else if (paused) {
            resume();
        }
    }

    private void crashNow(long downtimeMillis, String how) {
        trace.line(scheduler.now(), name, how + " (member " + memberId() + ", partitions " + consumers.keySet()
                + "), down for " + downtimeMillis + " ms");
        faults.count("matcher.crashes");
        coordinator.silent(memberId());
        down = true;
        paused = false;
        incarnation++;
        consumers.clear();
        matcher.forgetAll();
        deferred.clear();
        armedPauseMillis = 0;
        armedCrashDowntimeMillis = 0;
        int dead = incarnation;
        scheduler.after(downtimeMillis, name, () -> restart(dead));
    }

    private void restart(int deadIncarnation) {
        if (!down || deadIncarnation != incarnation) {
            return;
        }
        down = false;
        memberEpoch++;
        trace.line(scheduler.now(), name, "restarts as " + memberId());
        coordinator.join(memberId());
    }

    private void pause(long durationMillis) {
        armedPauseMillis = 0;
        paused = true;
        long token = ++pauseSeq;
        int pausedIncarnation = incarnation;
        faults.count("matcher.pauses");
        trace.line(scheduler.now(), name, "pauses for " + durationMillis + " ms holding "
                + consumers.values().stream().filter(c -> !c.held.isEmpty()).map(PartitionConsumer::describeHeld).toList());
        coordinator.silent(memberId());
        scheduler.after(durationMillis, name, () -> {
            if (pausedIncarnation == incarnation && paused && token == pauseSeq) {
                resume();
            }
        });
    }

    /** Wakes up: first the held batches (a zombie's are fenced), then held work, then a heartbeat. */
    private void resume() {
        paused = false;
        trace.line(scheduler.now(), name, "resumes as " + memberId());
        for (PartitionConsumer c : consumers.values()) {
            if (!c.held.isEmpty()) {
                c.pollScheduled = true;
                schedule(0, c::poll);
            }
        }
        List<Runnable> held = new ArrayList<>(deferred);
        deferred.clear();
        for (Runnable r : held) {
            schedule(0, r);
        }
        schedule(0, this::heartbeat);
    }

    private void heartbeat() {
        Optional<TreeSet<Integer>> owned = coordinator.heartbeat(memberId());
        TreeSet<Integer> keep = owned.orElseGet(TreeSet::new);
        for (Integer p : new ArrayList<>(consumers.keySet())) {
            if (!keep.contains(p)) {
                trace.line(scheduler.now(), name, "learns it lost p" + p);
                consumers.remove(p).active = false;
                matcher.ownershipChanged(p);
            }
        }
        if (owned.isEmpty()) {
            memberEpoch++;
            coordinator.join(memberId());
        }
    }

    private static String describe(BatchOutcome outcome) {
        if (outcome.raised().isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder(" raised=");
        outcome.raised().forEach(b -> out.append(b.type()).append('(').append(b.subjectKey()).append(") "));
        return out.toString().stripTrailing();
    }

    // ---- one partition ----------------------------------------------------------------------------------------

    private final class PartitionConsumer {
        private final int partition;
        private long epoch;
        private long position;
        private boolean active = true;
        private boolean pollScheduled;
        /** Scheduler time of the pending tick, or {@code Long.MAX_VALUE}. */
        private long tickAt = Long.MAX_VALUE;
        /** A fetched batch not yet processed: held across a pause. */
        private List<LogRecord> held = List.of();

        PartitionConsumer(int partition) {
            this.partition = partition;
        }

        String describeHeld() {
            return "p" + partition + "@" + held.get(0).offset() + ".." + held.get(held.size() - 1).offset()
                    + " epoch " + epoch;
        }

        void acquire() {
            if (!active) {
                return;
            }
            String member = memberId();
            try {
                PartitionRepo.Lease lease = store.transact(member + "-acquire-p" + partition,
                        tx -> tx.partitions().acquire(partition, member));
                epoch = lease.epoch();
                position = lease.nextOffset();
                matcher.ownershipChanged(partition);
                trace.line(scheduler.now(), name, "acquires p" + partition + " epoch " + epoch + " at offset " + position);
                warm();
                pollScheduled = true;
                long latency = faults.assignLatencyMillis();
                schedule(latency, this::poll);
                scheduleTickIn(latency + MIN_TICK_DELAY_MILLIS);
            } catch (StoreUnavailable e) {
                faults.count("store.retries");
                schedule(STORE_RETRY_MILLIS, this::acquire);
            }
        }

        /** Loads the partition's open payments and orders into the cache (§5.2); skipped while the store is down. */
        void warm() {
            try {
                KeyedState.Delta delta = store.transact(memberId() + "-warm-p" + partition, tx -> matcher.warm(partition, tx));
                matcher.committed(delta);
            } catch (StoreUnavailable e) {
                faults.count("store.retries");
            }
        }

        void scheduleTick(Optional<Instant> wakeAt) {
            wakeAt.ifPresent(at -> scheduleTickIn(at.toEpochMilli() - clock.now().toEpochMilli()));
        }

        void scheduleTickIn(long delayMillis) {
            long delay = Math.max(MIN_TICK_DELAY_MILLIS, delayMillis);
            long at = scheduler.now() + delay;
            if (tickAt > scheduler.now() && tickAt <= at) {
                return;   // an earlier tick is already pending
            }
            tickAt = at;
            schedule(delay, () -> {
                if (tickAt == at) {
                    tick();
                }
            });
        }

        void tick() {
            tickAt = Long.MAX_VALUE;
            if (!active || epoch == 0) {
                return;
            }
            String member = memberId();
            try {
                BatchOutcome outcome = store.transact(member + "-tick-p" + partition,
                        tx -> matcher.tick(partition, epoch, clock.now(), tx));
                matcher.committed(outcome);
                afterCommit.run();
                if (outcome.timersFired() > 0 || !outcome.raised().isEmpty()) {
                    trace.line(scheduler.now(), name, "p" + partition + " tick timers=" + outcome.timersFired()
                            + describe(outcome));
                }
                scheduleTick(outcome.nextWakeAt());
            } catch (StoreUnavailable e) {
                faults.count("store.retries");
                scheduleTickIn(STORE_RETRY_MILLIS);
            } catch (StoreConflict fenced) {
                fenced(fenced);
            }
        }

        private void fenced(StoreConflict fenced) {
            faults.count("matcher.fenced");
            trace.line(scheduler.now(), name, "fenced on p" + partition + " with epoch " + epoch + ": " + fenced.getMessage());
            active = false;
            consumers.remove(partition);
            matcher.ownershipChanged(partition);
            String member = memberId();
            schedule(REACQUIRE_AFTER_MILLIS, () -> {
                if (memberId().equals(member) && !consumers.containsKey(partition)
                        && coordinator.assignedTo(member, partition)) {
                    trace.line(scheduler.now(), name, "still assigned p" + partition + " after being fenced: acquires again");
                    faults.count("matcher.reacquired");
                    PartitionConsumer again = new PartitionConsumer(partition);
                    consumers.put(partition, again);
                    again.acquire();
                }
            });
        }

        void wake() {
            if (active && epoch > 0 && !pollScheduled) {
                pollScheduled = true;
                schedule(POLL_DELAY_MILLIS, this::poll);
            }
        }

        void poll() {
            pollScheduled = false;
            if (!active) {
                return;
            }
            List<LogRecord> records = held.isEmpty() ? log.read(partition, position, MAX_BATCH) : held;
            held = List.of();
            if (records.isEmpty()) {
                return;
            }
            if (armedPauseMillis > 0) {
                held = records;
                pause(armedPauseMillis);
                return;
            }
            boolean crashing = armedCrashDowntimeMillis > 0;
            long downtime = armedCrashDowntimeMillis;
            String member = memberId();
            try {
                BatchOutcome outcome = store.transact(member + "-batch-p" + partition, tx -> {
                    BatchOutcome o = matcher.process(new PartitionBatch(partition, epoch, clock.now(), records), tx);
                    if (crashing) {
                        throw new SimCrash();
                    }
                    return o;
                });
                position = records.get(records.size() - 1).offset() + 1;
                matcher.committed(outcome);
                afterCommit.run();
                trace.line(scheduler.now(), name, "p" + partition + " epoch " + epoch + " batch @" + records.get(0).offset()
                        + " applied=" + outcome.applied() + " skipped=" + outcome.alreadyProcessed()
                        + " dup=" + outcome.duplicates() + " reused=" + outcome.keyReused()
                        + " later=" + outcome.notYetHandled() + " posted=" + outcome.postings()
                        + " timers=" + outcome.timersFired() + describe(outcome));
                scheduleTick(outcome.nextWakeAt());
                for (var conflict : outcome.ledgerConflicts()) {
                    anomalies.accept("LEDGER_CONFLICT: " + conflict.existing() + " vs " + conflict.attempted());
                }
                if (position < log.endOffset(partition)) {
                    wake();
                }
            } catch (SimCrash crash) {
                faults.count("matcher.crashes.mid_batch");
                crashNow(downtime, "crashes mid-batch at p" + partition + "@" + records.get(0).offset() + ", rolled back");
            } catch (StoreUnavailable e) {
                if (crashing) {
                    crashNow(downtime, "crashes while the store is down");
                    return;
                }
                faults.count("store.retries");
                pollScheduled = true;
                schedule(STORE_RETRY_MILLIS, this::poll);
            } catch (StoreConflict fenced) {
                fenced(fenced);
            }
        }
    }
}
