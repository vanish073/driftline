package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
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
 */
public final class MatcherInstance {

    public static final int MAX_BATCH = 100;
    static final long POLL_DELAY_MILLIS = 50;
    static final long STORE_RETRY_MILLIS = 1_000;

    private final String name;
    private final Store store;
    private final SimLog log;
    private final Scheduler scheduler;
    private final Trace trace;
    private final Coordinator coordinator;
    private final FaultInjector faults;
    private final Consumer<String> anomalies;
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
                           FaultInjector faults, Canaries canaries, Consumer<String> anomalies) {
        this.name = name;
        this.store = store;
        this.log = log;
        this.scheduler = scheduler;
        this.trace = trace;
        this.coordinator = coordinator;
        this.faults = faults;
        this.anomalies = anomalies;
        this.matcher = new Matcher(canaries);
        this.clock = new SimClock(scheduler);
        coordinator.register(this);
    }

    public String name() {
        return name;
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
            }
        }
        if (owned.isEmpty()) {
            memberEpoch++;
            coordinator.join(memberId());
        }
    }

    // ---- one partition ----------------------------------------------------------------------------------------

    private final class PartitionConsumer {
        private final int partition;
        private long epoch;
        private long position;
        private boolean active = true;
        private boolean pollScheduled;
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
                trace.line(scheduler.now(), name, "acquires p" + partition + " epoch " + epoch + " at offset " + position);
                pollScheduled = true;
                schedule(faults.assignLatencyMillis(), this::poll);
            } catch (StoreUnavailable e) {
                faults.count("store.retries");
                schedule(STORE_RETRY_MILLIS, this::acquire);
            }
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
                    BatchOutcome o = matcher.process(new PartitionBatch(partition, epoch, records), tx);
                    if (crashing) {
                        throw new SimCrash();
                    }
                    return o;
                });
                position = records.get(records.size() - 1).offset() + 1;
                trace.line(scheduler.now(), name, "p" + partition + " epoch " + epoch + " batch @" + records.get(0).offset()
                        + " applied=" + outcome.applied() + " skipped=" + outcome.alreadyProcessed()
                        + " dup=" + outcome.duplicates() + " reused=" + outcome.keyReused()
                        + " later=" + outcome.notYetHandled() + " posted=" + outcome.postings());
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
                faults.count("matcher.fenced");
                trace.line(scheduler.now(), name, "fenced on p" + partition + " with epoch " + epoch + ": " + fenced.getMessage());
                active = false;
                consumers.remove(partition);
            }
        }
    }
}
