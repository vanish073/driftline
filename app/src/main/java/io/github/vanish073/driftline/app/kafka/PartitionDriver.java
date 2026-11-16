package io.github.vanish073.driftline.app.kafka;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.KeyedState;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.PartitionBatch;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.PartitionRepo;
import io.github.vanish073.driftline.core.port.QuarantineRepo;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.core.port.Tx;
import io.github.vanish073.driftline.wire.envelope.EnvelopeCodec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The Kafka partition driver (DESIGN §3.1, §5.1, §5.7, D19, D50): plain kafka-clients on one dedicated thread, the
 * production twin of the simulator's {@code MatcherInstance}. Kafka's group decides who <em>should</em> own a partition;
 * the epoch in PostgreSQL decides who <em>may commit</em> for it.
 *
 * <ul>
 *   <li><b>Assignment.</b> A newly assigned partition is paused, then acquired in the poll loop: the epoch is bumped
 *       ({@code acquire}), the keyed state is dropped and warmed, and the consumer seeks to the stored offset before
 *       the partition is resumed. Nothing blocks inside the rebalance callback.</li>
 *   <li><b>One transaction per polled chunk.</b> Each partition's records from one poll go to the matcher in one Store
 *       transaction, offsets included (D2). A record that does not decode is quarantined inside that same transaction
 *       ({@link Matcher#quarantineUndecodable}), so the partition never stalls on it and nothing is skipped.</li>
 *   <li><b>Fencing.</b> A commit under a stale epoch throws {@link StoreConflict}: the whole transaction rolls back,
 *       the partition is paused and its cache dropped (a zombie, §5.1). If Kafka still assigns it to this instance
 *       after {@code reacquireAfter}, the instance acquires it again, which is then correct.</li>
 *   <li><b>Ticks.</b> At the matcher's {@code nextWakeAt}, and at least every {@code maxTickInterval}, a tick advances
 *       idle watermarks and fires due timers (§5.4, §5.5), fenced like a batch.</li>
 *   <li><b>Failures.</b> The store being down means the chunk is re-read from its first offset after a pause. Any
 *       other exception stops the driver (fail-stop): it is reported by the health check so the process is restarted,
 *       and the restart replays from the stored offset. Graceful shutdown finishes the current chunk, then leaves the
 *       group (§10.4).</li>
 * </ul>
 *
 * <p>Crash points (§9.4): {@code CP7} is reached before a chunk's transaction (a test pauses there to make a zombie);
 * {@code CP6} inside it, after the matcher ran and before the commit.
 */
public final class PartitionDriver implements SmartLifecycle {

    /** What the driver did; tests wait on it, and production counts it. */
    public interface Listener {
        Listener NONE = new Listener() { };

        default void acquired(int partition, long epoch, long nextOffset) {
        }

        default void committed(int partition, List<BatchOutcome> outcomes, int quarantined) {
        }

        default void fenced(int partition) {
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PartitionDriver.class);
    private static final Duration STORE_RETRY = Duration.ofSeconds(1);
    private static final Duration FIRST_TICK = Duration.ofSeconds(1);

    private final Store store;
    private final Matcher matcher;
    private final Clock clock;
    private final Supplier<Consumer<byte[], byte[]>> consumers;
    private final String topic;
    private final String owner;
    private final CrashPoints crashPoints;
    private final Listener listener;
    private final Duration pollTimeout;
    private final Duration maxTickInterval;
    private final Duration reacquireAfter;

    // Touched only by the driver thread.
    private final TreeMap<Integer, Owned> owned = new TreeMap<>();
    private final TreeSet<Integer> toAcquire = new TreeSet<>();
    private final TreeMap<Integer, Instant> fenced = new TreeMap<>();
    private Consumer<byte[], byte[]> consumer;

    private volatile boolean running;
    private volatile Thread thread;
    private volatile Throwable failure;

    private static final class Owned {
        final long epoch;
        Instant nextTickAt;

        Owned(long epoch, Instant nextTickAt) {
            this.epoch = epoch;
            this.nextTickAt = nextTickAt;
        }
    }

    public PartitionDriver(Store store, Matcher matcher, Clock clock, Supplier<Consumer<byte[], byte[]>> consumers,
                           String topic, String owner, CrashPoints crashPoints, Listener listener, Duration pollTimeout,
                           Duration maxTickInterval, Duration reacquireAfter) {
        this.store = Objects.requireNonNull(store, "store");
        this.matcher = Objects.requireNonNull(matcher, "matcher");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.consumers = Objects.requireNonNull(consumers, "consumers");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.crashPoints = Objects.requireNonNull(crashPoints, "crashPoints");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.pollTimeout = pollTimeout;
        this.maxTickInterval = maxTickInterval;
        this.reacquireAfter = reacquireAfter;
    }

    // ---- lifecycle ------------------------------------------------------------------------------------------------

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        failure = null;
        thread = Thread.ofPlatform().name("partition-driver-" + owner).start(this::run);
    }

    @Override
    public void stop() {
        Thread t;
        synchronized (this) {
            running = false;
            t = thread;
            Consumer<byte[], byte[]> c = consumer;
            if (c != null) {
                c.wakeup();
            }
        }
        if (t != null) {
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

    /** Why the driver stopped on its own, if it did (the health check reports it). */
    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure);
    }

    /** The partitions this instance holds a current epoch for. Safe to call from any thread (a snapshot). */
    public synchronized List<Integer> ownedPartitions() {
        return List.copyOf(owned.keySet());
    }

    // ---- the loop -------------------------------------------------------------------------------------------------

    private void run() {
        try (Consumer<byte[], byte[]> c = consumers.get()) {
            synchronized (this) {
                consumer = c;
                if (!running) {
                    return;
                }
            }
            c.subscribe(List.of(topic), new Rebalance());
            while (running) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = c.poll(pollTimeout);
                } catch (WakeupException e) {
                    continue;   // stop() woke us: the loop condition decides
                }
                for (TopicPartition tp : records.partitions()) {
                    process(tp, records.records(tp));
                }
                acquirePending();
                tickDue();
                reacquireFenced();
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            LOG.error("partition driver {} stopped; the process must restart and will replay from the stored offsets",
                    owner, e);
        } finally {
            synchronized (this) {
                running = false;
                consumer = null;
                for (Integer p : owned.keySet()) {
                    matcher.ownershipChanged(p);
                }
                owned.clear();
                toAcquire.clear();
                fenced.clear();
            }
        }
    }

    private final class Rebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            consumer.pause(partitions);   // resumed after acquire and seek
            for (TopicPartition tp : partitions) {
                toAcquire.add(tp.partition());
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            drop(partitions, "revoked");
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            drop(partitions, "lost");
        }
    }

    private void drop(Collection<TopicPartition> partitions, String how) {
        synchronized (this) {
            for (TopicPartition tp : partitions) {
                int p = tp.partition();
                owned.remove(p);
                toAcquire.remove(p);
                fenced.remove(p);
                matcher.ownershipChanged(p);
            }
        }
        if (!partitions.isEmpty()) {
            LOG.info("{}: partitions {} {}", owner, partitions.stream().map(TopicPartition::partition).toList(), how);
        }
    }

    private TopicPartition tp(int partition) {
        return new TopicPartition(topic, partition);
    }

    private void acquirePending() {
        for (Integer p : List.copyOf(toAcquire)) {
            if (!consumer.assignment().contains(tp(p))) {
                toAcquire.remove(p);
                continue;
            }
            try {
                acquire(p);
                toAcquire.remove(p);
            } catch (StoreUnavailable e) {
                LOG.warn("{}: cannot acquire p{} while the store is unavailable; retrying", owner, p);
                pause(STORE_RETRY);
                return;
            }
        }
    }

    /** §5.1: bump the epoch, drop and warm the cache, seek to the stored offset, resume. */
    private void acquire(int p) {
        PartitionRepo.Lease lease = store.transact(owner + "-acquire-p" + p, tx -> tx.partitions().acquire(p, owner));
        matcher.ownershipChanged(p);
        KeyedState.Delta warm = store.transact(owner + "-warm-p" + p, tx -> matcher.warm(p, tx));
        matcher.committed(warm);
        synchronized (this) {
            owned.put(p, new Owned(lease.epoch(), clock.now().plus(FIRST_TICK)));
            fenced.remove(p);
        }
        consumer.seek(tp(p), lease.nextOffset());
        consumer.resume(List.of(tp(p)));
        LOG.info("{}: acquired p{} epoch {} at offset {}", owner, p, lease.epoch(), lease.nextOffset());
        listener.acquired(p, lease.epoch(), lease.nextOffset());
    }

    // ---- one polled chunk -----------------------------------------------------------------------------------------

    /** One record as read: decoded, or kept raw for the quarantine. */
    private record Item(long offset, LogRecord record, String error, byte[] raw) {
    }

    private Item decode(int p, ConsumerRecord<byte[], byte[]> r) {
        byte[] value = r.value();
        if (value == null) {
            return new Item(r.offset(), null, "the record has no value", new byte[0]);
        }
        try {
            EventEnvelope e = EnvelopeCodec.decode(value);
            if (e.partition() != p) {
                return new Item(r.offset(), null, "the envelope names partition " + e.partition()
                        + " but was read from partition " + p, value);
            }
            return new Item(r.offset(), new LogRecord(p, r.offset(), e), null, null);
        } catch (IllegalArgumentException e) {   // MalformedJson included; messages name fields, never values
            String error = e.getMessage() == null ? "undecodable" : e.getMessage();
            return new Item(r.offset(), null, error.length() > QuarantineRepo.Undecodable.MAX_ERROR_LENGTH
                    ? error.substring(0, QuarantineRepo.Undecodable.MAX_ERROR_LENGTH) : error, value);
        }
    }

    private void process(TopicPartition tp, List<ConsumerRecord<byte[], byte[]>> records) {
        int p = tp.partition();
        Owned o = owned.get(p);
        if (o == null || records.isEmpty()) {
            return;   // not (or no longer) ours; a later acquire seeks to the stored offset anyway
        }
        List<Item> items = new ArrayList<>(records.size());
        for (ConsumerRecord<byte[], byte[]> r : records) {
            items.add(decode(p, r));
        }
        Instant now = clock.now();
        long epoch = o.epoch;
        int[] quarantined = {0};
        crashPoints.reached(CrashPoint.CP7);
        try {
            List<BatchOutcome> outcomes = store.transact(owner + "-batch-p" + p, tx -> {
                List<BatchOutcome> out = new ArrayList<>();
                List<LogRecord> run = new ArrayList<>();
                for (Item item : items) {
                    if (item.record() != null) {
                        run.add(item.record());
                        continue;
                    }
                    flush(p, epoch, now, run, out, tx);
                    byte[] raw = item.raw().length > QuarantineRepo.Undecodable.MAX_RAW_BYTES
                            ? Arrays.copyOf(item.raw(), QuarantineRepo.Undecodable.MAX_RAW_BYTES) : item.raw();
                    if (matcher.quarantineUndecodable(p, epoch, item.offset(), item.error(), raw, tx)) {
                        quarantined[0]++;
                    }
                }
                flush(p, epoch, now, run, out, tx);
                crashPoints.reached(CrashPoint.CP6);
                return out;
            });
            Instant next = now.plus(maxTickInterval);
            for (BatchOutcome outcome : outcomes) {
                matcher.committed(outcome);
                if (outcome.nextWakeAt().isPresent() && outcome.nextWakeAt().get().isBefore(next)) {
                    next = outcome.nextWakeAt().get();
                }
                for (var conflict : outcome.ledgerConflicts()) {
                    LOG.error("LEDGER_CONFLICT on p{}: {} vs {}", p, conflict.existing(), conflict.attempted());
                }
            }
            if (next.isBefore(o.nextTickAt)) {
                o.nextTickAt = next;
            }
            if (quarantined[0] > 0) {
                LOG.warn("{}: quarantined {} undecodable records on p{}", owner, quarantined[0], p);
            }
            listener.committed(p, outcomes, quarantined[0]);
        } catch (StoreUnavailable e) {
            LOG.warn("{}: store unavailable on p{}; re-reading from offset {}", owner, p, records.get(0).offset());
            consumer.seek(tp, records.get(0).offset());
            pause(STORE_RETRY);
        } catch (StoreConflict e) {
            fence(p, now, e);
        }
    }

    private void flush(int p, long epoch, Instant now, List<LogRecord> run, List<BatchOutcome> out, Tx tx) {
        if (!run.isEmpty()) {
            out.add(matcher.process(new PartitionBatch(p, epoch, now, List.copyOf(run)), tx));
            run.clear();
        }
    }

    private void fence(int p, Instant now, StoreConflict cause) {
        synchronized (this) {
            owned.remove(p);
            fenced.put(p, now);
        }
        matcher.ownershipChanged(p);
        consumer.pause(List.of(tp(p)));
        LOG.warn("{}: fenced on p{}; rolled back and paused ({})", owner, p, cause.getMessage());
        listener.fenced(p);
    }

    private void reacquireFenced() {
        Instant now = clock.now();
        for (var e : List.copyOf(fenced.entrySet())) {
            int p = e.getKey();
            if (!consumer.assignment().contains(tp(p))) {
                fenced.remove(p);   // Kafka moved it: the new owner has it
            } else if (!now.isBefore(e.getValue().plus(reacquireAfter))) {
                fenced.remove(p);
                toAcquire.add(p);   // Kafka still says it is ours: take it back properly
            }
        }
    }

    // ---- ticks ----------------------------------------------------------------------------------------------------

    private void tickDue() {
        Instant now = clock.now();
        for (Integer p : List.copyOf(owned.keySet())) {
            Owned o = owned.get(p);
            if (o == null || o.nextTickAt.isAfter(now)) {
                continue;
            }
            try {
                BatchOutcome outcome = store.transact(owner + "-tick-p" + p, tx -> matcher.tick(p, o.epoch, now, tx));
                matcher.committed(outcome);
                Instant next = now.plus(maxTickInterval);
                if (outcome.nextWakeAt().isPresent() && outcome.nextWakeAt().get().isBefore(next)) {
                    next = outcome.nextWakeAt().get();
                }
                o.nextTickAt = next;
                listener.committed(p, List.of(outcome), 0);
            } catch (StoreUnavailable e) {
                o.nextTickAt = now.plus(STORE_RETRY);
            } catch (StoreConflict e) {
                fence(p, now, e);
            }
        }
    }

    private static void pause(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
