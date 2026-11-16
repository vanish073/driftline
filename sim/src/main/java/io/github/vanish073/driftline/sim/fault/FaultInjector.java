package io.github.vanish073.driftline.sim.fault;

import io.github.vanish073.driftline.sim.random.SplitMix64;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Decides every fault of one run from the {@code faults} stream (DESIGN §11.2, §11.5). Rate faults are rolled when
 * a message is sent; event faults are planned up front for the RUN phase. Every roll consumes the same draws whether
 * its fault is on or off, so switching a fault off does not shift the others. After {@link #heal()} (the HEAL phase,
 * §11.6) no fault fires any more.
 *
 * <p>The bounds below are the liveness assumptions of §11.6: every outage is far shorter than the world's delivery
 * horizon (3 days), so a run that heals must drain.
 */
public final class FaultInjector {

    public static final long MAX_DOWNTIME_MILLIS = 10 * 60_000L;
    public static final long MAX_STORE_DOWN_MILLIS = 5 * 60_000L;
    public static final long MAX_SKEW_MILLIS = 120_000L;
    public static final long MAX_JUMP_FORWARD_MILLIS = 15 * 60_000L;
    public static final long MAX_JUMP_BACK_MILLIS = 2_000L;
    public static final long MAX_JUMP_DURATION_MILLIS = 20 * 60_000L;
    /** Heavy-tailed delays: 200 ms × 4^k with P(k) = 2^-(k+1), capped at 10 minutes. */
    public static final long HEAVY_DELAY_BASE_MILLIS = 200;
    public static final long MAX_HEAVY_DELAY_MILLIS = 10 * 60_000L;

    private final Swarm swarm;
    private final SplitMix64 rng;
    private final TreeMap<String, Long> counters = new TreeMap<>();
    private boolean healed;

    public FaultInjector(Swarm swarm, SplitMix64 rng) {
        this.swarm = swarm;
        this.rng = rng;
    }

    public Swarm swarm() {
        return swarm;
    }

    /** The HEAL phase starts: the network becomes reliable and no fault fires any more. */
    public void heal() {
        healed = true;
    }

    public boolean healed() {
        return healed;
    }

    private boolean rate(Fault f, String counter) {
        return rate(f, counter, true);
    }

    private boolean rate(Fault f, String counter, boolean eligible) {
        long roll = rng.nextLong(1_000);
        boolean hit = eligible && !healed && roll < swarm.level(f);
        if (hit) {
            count(counter);
        }
        return hit;
    }

    public boolean drop() {
        return rate(Fault.NET_DROP, "net.dropped");
    }

    public boolean duplicate() {
        return rate(Fault.NET_DUP, "net.duplicated");
    }

    public boolean produceAmbiguous() {
        return rate(Fault.PRODUCE_AMBIGUOUS, "ingest.ambiguous");
    }

    /** @param eligible false when there is no earlier key to reuse; the roll is still drawn */
    public boolean reuseKey(boolean eligible) {
        return rate(Fault.MERCHANT_KEY_REUSE, "merchant.keys.reused", eligible);
    }

    /** CALL_AMBIGUOUS: the gateway applied this refund, and its response is lost. */
    public boolean callAmbiguous() {
        return rate(Fault.CALL_AMBIGUOUS, "gateway.ambiguous");
    }

    /** WEBHOOK_LOST: the gateway gives up on this webhook and never delivers it. */
    public boolean webhookLost() {
        return rate(Fault.WEBHOOK_LOST, "gateway.webhooks.lost");
    }

    public static final long MIN_STATUS_LAG_MILLIS = 60_000L;
    public static final long MAX_STATUS_LAG_MILLIS = 10 * 60_000L;

    /** STATUS_STALE: 0, or how far behind reality this status answer is. */
    public long statusLagMillis() {
        boolean hit = rate(Fault.STATUS_STALE, "gateway.status.stale");
        long lag = rng.between(MIN_STATUS_LAG_MILLIS, MAX_STATUS_LAG_MILLIS);
        return hit ? lag : 0;
    }

    /** Extra delay for one message: 0, or a heavy-tailed amount when NET_DELAY strikes. */
    public long extraDelay() {
        boolean hit = rate(Fault.NET_DELAY, "net.delayed");
        long bits = rng.nextLong();
        long jitter = rng.nextLong(1L << 40);
        if (!hit) {
            return 0;
        }
        int k = Math.min(5, Long.numberOfTrailingZeros(bits | (1L << 62)));
        long low = HEAVY_DELAY_BASE_MILLIS << (2 * k);
        long delay = low + jitter % (3 * low);
        return Math.min(delay, MAX_HEAVY_DELAY_MILLIS);
    }

    /** How long FILE_LATE holds a settlement file back: at most 36 hours, far inside the 3-day delivery horizon. */
    public static final long MIN_FILE_DELAY_MILLIS = 6 * 3_600_000L;
    public static final long MAX_FILE_DELAY_MILLIS = 36 * 3_600_000L;

    /** A per-file roll that HEAL does not stop: the bank's feed misbehaves on its own schedule. */
    private boolean fileRate(Fault f, String counter) {
        long roll = rng.nextLong(1_000);
        boolean hit = roll < swarm.level(f);
        if (hit) {
            count(counter);
        }
        return hit;
    }

    /** FILE_LATE: 0, or how long this settlement file is held back. */
    public long fileDelay() {
        boolean hit = fileRate(Fault.FILE_LATE, "file.late");
        long delay = rng.between(MIN_FILE_DELAY_MILLIS, MAX_FILE_DELAY_MILLIS);
        return hit ? delay : 0;
    }

    /** FILE_DUP: whether this settlement file is uploaded a second time. */
    public boolean fileDuplicate() {
        return fileRate(Fault.FILE_DUP, "file.duplicated");
    }

    /** FILE_CONFLICT: whether a different file for this date follows once this one is accepted. */
    public boolean fileConflict() {
        return fileRate(Fault.FILE_CONFLICT, "file.conflicting");
    }

    /** How long a newly assigned partition takes to start fetching (rebalance and state load). Not a fault. */
    public long assignLatencyMillis() {
        return rng.between(500L, 5_000L);
    }

    /** Each node's fixed clock offset under CLOCK_SKEW; 0 for every node otherwise. */
    public TreeMap<String, Long> skews(List<String> nodes) {
        TreeMap<String, Long> out = new TreeMap<>();
        for (String node : nodes) {
            long offset = rng.between(-MAX_SKEW_MILLIS, MAX_SKEW_MILLIS);
            out.put(node, swarm.on(Fault.CLOCK_SKEW) ? offset : 0L);
        }
        return out;
    }

    /**
     * Plans the event faults of the RUN phase {@code [0, runMillis)}, ordered by time.
     *
     * @param crashable  nodes that may crash (ingest and the matchers)
     * @param pausable   nodes that may pause (the matchers)
     * @param clocked    nodes with a clock that may jump
     */
    public List<FaultEvent> plan(long runMillis, int trafficDays, List<String> crashable, List<String> pausable,
                                 List<String> clocked) {
        List<FaultEvent> events = new ArrayList<>();
        for (Fault f : List.of(Fault.NODE_CRASH, Fault.NODE_PAUSE, Fault.STORE_DOWN, Fault.CLOCK_JUMP)) {
            int count = swarm.level(f) * trafficDays;
            for (int i = 0; i < count; i++) {
                long at = rng.between(1L, runMillis - 1);
                switch (f) {
                    case NODE_CRASH -> events.add(new FaultEvent.Crash(at, rng.pick(crashable),
                            rng.between(1_000L, MAX_DOWNTIME_MILLIS), rng.chance(1, 2)));
                    // Short pauses end before the session timeout; long ones outlast it and leave a zombie.
                    case NODE_PAUSE -> events.add(new FaultEvent.Pause(at, rng.pick(pausable),
                            rng.between(1_000L, PAUSE_MAX_MILLIS)));
                    case STORE_DOWN -> events.add(new FaultEvent.StoreDown(at, rng.between(1_000L, MAX_STORE_DOWN_MILLIS)));
                    case CLOCK_JUMP -> {
                        String node = rng.pick(clocked);
                        long step = rng.chance(7, 10) ? rng.between(1_000L, MAX_JUMP_FORWARD_MILLIS)
                                : -rng.between(1L, MAX_JUMP_BACK_MILLIS);
                        events.add(new FaultEvent.ClockJump(at, node, step, rng.between(60_000L, MAX_JUMP_DURATION_MILLIS)));
                    }
                    default -> throw new IllegalStateException(f + " is not an event fault");
                }
            }
        }
        events.sort(Comparator.comparingLong(FaultEvent::atMillis));
        for (FaultEvent e : events) {
            count("planned." + e.getClass().getSimpleName());
        }
        return List.copyOf(events);
    }

    /** The longest pause: past the session timeout plus the slowest partition assignment, so zombies race new owners. */
    public static final long PAUSE_MAX_MILLIS = 30_000L;

    public void count(String counter) {
        counters.merge(counter, 1L, Long::sum);
    }

    public Map<String, Long> counters() {
        return new TreeMap<>(counters);
    }
}
