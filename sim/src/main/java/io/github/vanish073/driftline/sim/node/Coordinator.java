package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.Trace;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The consumer-group coordinator in the simulator (the role Kafka's group coordinator plays; DESIGN §5.1, §11.5).
 * It maps each partition to one member. A member that stops heartbeating (crashed or paused) is expired after
 * {@link #SESSION_TIMEOUT_MILLIS}; its partitions go to the live member with the fewest, or wait as orphans until a
 * member joins. Assignment is sticky: a member that joins takes orphans only, never a partition someone else holds.
 *
 * <p>The coordinator never fences anyone itself. The new owner's {@code acquire} bumps the epoch in the Store, and
 * that is what stops a zombie (§5.1).
 */
public final class Coordinator {

    public static final long SESSION_TIMEOUT_MILLIS = 10_000;

    private final Scheduler scheduler;
    private final Trace trace;
    private final int partitions;
    private final TreeMap<String, MatcherInstance> instances = new TreeMap<>();
    /** partition → member id, for assigned partitions only. */
    private final TreeMap<Integer, String> owners = new TreeMap<>();
    /** member id → live; a member id is {@code instance.memberEpoch}. */
    private final TreeSet<String> live = new TreeSet<>();
    private final TreeMap<String, Long> silence = new TreeMap<>();
    private long silenceSeq;
    private long reassignments;

    public Coordinator(Scheduler scheduler, Trace trace, int partitions) {
        this.scheduler = scheduler;
        this.trace = trace;
        this.partitions = partitions;
    }

    void register(MatcherInstance instance) {
        instances.put(instance.name(), instance);
    }

    /** Initial assignment: partition p to the (p mod n)-th instance. */
    public void start() {
        List<MatcherInstance> all = new ArrayList<>(instances.values());
        for (MatcherInstance i : all) {
            live.add(i.memberId());
        }
        for (int p = 0; p < partitions; p++) {
            MatcherInstance to = all.get(p % all.size());
            owners.put(p, to.memberId());
            to.assign(p);
        }
    }

    /** The member that should process {@code partition} now, if any. */
    public Optional<MatcherInstance> ownerOf(int partition) {
        String member = owners.get(partition);
        return member == null ? Optional.empty() : Optional.ofNullable(instances.get(instanceOf(member)));
    }

    /** Wakes the owner of {@code partition}: new records were appended. */
    public void wake(int partition) {
        ownerOf(partition).ifPresent(i -> i.wake(partition));
    }

    /** {@code memberId} stopped heartbeating; it is expired unless it comes back within the session timeout. */
    void silent(String memberId) {
        long token = ++silenceSeq;
        silence.put(memberId, token);
        scheduler.after(SESSION_TIMEOUT_MILLIS, "coordinator", () -> expire(memberId, token));
    }

    /**
     * {@code memberId} heartbeats again.
     *
     * @return the partitions it still owns, or empty if it was expired and must rejoin under a new member id
     */
    Optional<TreeSet<Integer>> heartbeat(String memberId) {
        silence.remove(memberId);
        if (!live.contains(memberId)) {
            return Optional.empty();
        }
        TreeSet<Integer> owned = new TreeSet<>();
        owners.forEach((p, m) -> {
            if (m.equals(memberId)) {
                owned.add(p);
            }
        });
        return Optional.of(owned);
    }

    /** A new member joins (a restart, or a zombie rejoining under a new member id) and takes any orphans. */
    void join(String memberId) {
        live.add(memberId);
        trace.line(scheduler.now(), "coordinator", "join " + memberId);
        for (int p = 0; p < partitions; p++) {
            if (!owners.containsKey(p)) {
                assign(p);
            }
        }
    }

    private void expire(String memberId, long token) {
        Long current = silence.get(memberId);
        if (current == null || current != token || !live.contains(memberId)) {
            return;
        }
        silence.remove(memberId);
        live.remove(memberId);
        List<Integer> lost = new ArrayList<>();
        owners.forEach((p, m) -> {
            if (m.equals(memberId)) {
                lost.add(p);
            }
        });
        trace.line(scheduler.now(), "coordinator", "expire " + memberId + " partitions=" + lost);
        for (int p : lost) {
            owners.remove(p);
            assign(p);
        }
    }

    private void assign(int partition) {
        String best = null;
        long bestLoad = Long.MAX_VALUE;
        for (String member : live) {
            long load = owners.values().stream().filter(member::equals).count();
            if (load < bestLoad) {
                best = member;
                bestLoad = load;
            }
        }
        if (best == null) {
            trace.line(scheduler.now(), "coordinator", "p" + partition + " orphaned: no live member");
            return;
        }
        owners.put(partition, best);
        reassignments++;
        trace.line(scheduler.now(), "coordinator", "assign p" + partition + " -> " + best);
        instances.get(instanceOf(best)).assign(partition);
    }

    private static String instanceOf(String memberId) {
        return memberId.substring(0, memberId.lastIndexOf('.'));
    }

    public long reassignments() {
        return reassignments;
    }

    /** True when every partition has a live owner (checked at the end of a run). */
    public boolean allAssigned() {
        return owners.size() == partitions;
    }
}
