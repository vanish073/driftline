package io.github.vanish073.driftline.sim.fault;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.sim.node.Coordinator;
import io.github.vanish073.driftline.sim.random.SplitMix64;
import io.github.vanish073.driftline.sim.world.Delivery;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** The swarm vector and the fault injector keep to the bounds that DESIGN §11.5 and §11.6 rely on. */
class FaultInjectorTest {

    private static final List<String> NODES = List.of("ingest", "matcher-a", "matcher-b");
    private static final List<String> MATCHERS = List.of("matcher-a", "matcher-b");

    private static Map<Fault, Integer> all(int level) {
        EnumMap<Fault, Integer> m = new EnumMap<>(Fault.class);
        for (Fault f : Fault.values()) {
            m.put(f, level);
        }
        return m;
    }

    @Test
    void eachFaultIsOnInAboutHalfTheSeedsWithinItsRange() {
        int seeds = 4_000;
        EnumMap<Fault, Integer> on = new EnumMap<>(Fault.class);
        for (long seed = 1; seed <= seeds; seed++) {
            Swarm s = Swarm.draw(SplitMix64.stream(seed, "swarm"));
            for (Fault f : Fault.values()) {
                if (!s.on(f)) {
                    assertThat(s.level(f)).isZero();
                    continue;
                }
                on.merge(f, 1, Integer::sum);
                if (f.isRate()) {
                    assertThat(s.level(f)).as(f.name()).isBetween(10, 300);
                } else if (f == Fault.CLOCK_SKEW) {
                    assertThat(s.level(f)).isEqualTo(1);
                } else {
                    assertThat(s.level(f)).as(f.name()).isBetween(1, 4);
                }
            }
        }
        for (Fault f : Fault.values()) {
            assertThat(on.getOrDefault(f, 0)).as(f.name()).isBetween(seeds * 45 / 100, seeds * 55 / 100);
        }
    }

    @Test
    void theSwarmVectorIsAFunctionOfTheSeed() {
        assertThat(Swarm.draw(SplitMix64.stream(42, "swarm")).toString())
                .isEqualTo(Swarm.draw(SplitMix64.stream(42, "swarm")).toString());
        assertThat(Swarm.NONE.toString()).isEqualTo("none");
    }

    @Test
    void switchingOneFaultOffDoesNotShiftTheOthers() {
        FaultInjector both = new FaultInjector(Swarm.of(Map.of(Fault.NET_DROP, 300, Fault.NET_DUP, 300,
                Fault.NET_DELAY, 300)), new SplitMix64(7));
        FaultInjector dupOnly = new FaultInjector(Swarm.of(Map.of(Fault.NET_DUP, 300, Fault.NET_DELAY, 300)),
                new SplitMix64(7));
        int dropped = 0;
        for (int i = 0; i < 2_000; i++) {
            dropped += both.drop() ? 1 : 0;
            assertThat(dupOnly.drop()).isFalse();
            assertThat(dupOnly.duplicate()).as("roll %d", i).isEqualTo(both.duplicate());
            assertThat(dupOnly.extraDelay()).as("delay %d", i).isEqualTo(both.extraDelay());
        }
        assertThat(dropped).as("NET_DROP really fired where it was on").isPositive();
    }

    @Test
    void afterHealNothingFires() {
        FaultInjector f = new FaultInjector(Swarm.of(all(1_000)), new SplitMix64(3));
        assertThat(f.drop()).isTrue();
        assertThat(f.duplicate()).isTrue();
        assertThat(f.extraDelay()).isPositive();
        f.heal();
        for (int i = 0; i < 1_000; i++) {
            assertThat(f.drop() || f.duplicate() || f.produceAmbiguous() || f.reuseKey(true)).isFalse();
            assertThat(f.extraDelay()).isZero();
        }
        assertThat(f.healed()).isTrue();
    }

    @Test
    void keyReuseNeedsAnEarlierKey() {
        FaultInjector f = new FaultInjector(Swarm.of(Map.of(Fault.MERCHANT_KEY_REUSE, 1_000)), new SplitMix64(5));
        assertThat(f.reuseKey(false)).isFalse();
        assertThat(f.reuseKey(true)).isTrue();
    }

    @Test
    void heavyTailedDelaysStayUnderTheCap() {
        FaultInjector f = new FaultInjector(Swarm.of(Map.of(Fault.NET_DELAY, 1_000)), new SplitMix64(11));
        TreeMap<Long, Integer> buckets = new TreeMap<>();
        for (int i = 0; i < 20_000; i++) {
            long d = f.extraDelay();
            assertThat(d).isBetween(FaultInjector.HEAVY_DELAY_BASE_MILLIS, FaultInjector.MAX_HEAVY_DELAY_MILLIS);
            buckets.merge(Long.numberOfTrailingZeros(Long.highestOneBit(d)) / 2L, 1, Integer::sum);
        }
        assertThat(buckets.firstEntry().getValue()).as("most delays are short").isGreaterThan(buckets.lastEntry().getValue());
        assertThat(buckets.size()).as("but the tail is long").isGreaterThanOrEqualTo(5);
    }

    @Test
    void skewIsZeroUnlessClockSkewIsOn() {
        assertThat(new FaultInjector(Swarm.NONE, new SplitMix64(1)).skews(NODES).values()).containsOnly(0L);
        TreeMap<String, Long> skews = new FaultInjector(Swarm.of(Map.of(Fault.CLOCK_SKEW, 1)), new SplitMix64(1)).skews(NODES);
        assertThat(skews).containsOnlyKeys(NODES);
        assertThat(skews.values()).allSatisfy(s -> assertThat(Math.abs(s)).isLessThanOrEqualTo(FaultInjector.MAX_SKEW_MILLIS));
    }

    @Test
    void eventFaultsArePlannedInsideTheRunPhaseAndWithinTheirBounds() {
        long run = 2 * 86_400_000L;
        for (long seed = 1; seed <= 200; seed++) {
            FaultInjector f = new FaultInjector(Swarm.of(all(4)), new SplitMix64(seed));
            List<FaultEvent> plan = f.plan(run, 2, NODES, MATCHERS, NODES);
            assertThat(plan).hasSize(4 * 2 * 4);
            assertThat(plan).isSortedAccordingTo((a, b) -> Long.compare(a.atMillis(), b.atMillis()));
            for (FaultEvent e : plan) {
                assertThat(e.atMillis()).isBetween(1L, run - 1);
                switch (e) {
                    case FaultEvent.Crash c -> {
                        assertThat(NODES).contains(c.node());
                        assertThat(c.downtimeMillis()).isBetween(1_000L, FaultInjector.MAX_DOWNTIME_MILLIS);
                    }
                    case FaultEvent.Pause p -> {
                        assertThat(MATCHERS).contains(p.node());
                        assertThat(p.durationMillis()).isBetween(1_000L, FaultInjector.PAUSE_MAX_MILLIS);
                    }
                    case FaultEvent.StoreDown d ->
                            assertThat(d.durationMillis()).isBetween(1_000L, FaultInjector.MAX_STORE_DOWN_MILLIS);
                    case FaultEvent.ClockJump j -> {
                        assertThat(j.stepMillis()).isBetween(-FaultInjector.MAX_JUMP_BACK_MILLIS,
                                FaultInjector.MAX_JUMP_FORWARD_MILLIS).isNotZero();
                        assertThat(j.durationMillis()).isBetween(60_000L, FaultInjector.MAX_JUMP_DURATION_MILLIS);
                    }
                }
            }
        }
        assertThat(new FaultInjector(Swarm.NONE, new SplitMix64(1)).plan(run, 2, NODES, MATCHERS, NODES)).isEmpty();
    }

    /** §11.6 liveness: every outage is far shorter than the delivery horizon, and some pauses outlast a session. */
    @Test
    void theBoundsLeaveRoomToDrainAndToMakeZombies() {
        long longestOutage = Math.max(Math.max(FaultInjector.MAX_DOWNTIME_MILLIS, FaultInjector.MAX_STORE_DOWN_MILLIS),
                Math.max(FaultInjector.MAX_HEAVY_DELAY_MILLIS, FaultInjector.MAX_JUMP_DURATION_MILLIS));
        assertThat(longestOutage * 100).isLessThan(Delivery.HORIZON_MILLIS);
        assertThat(FaultInjector.PAUSE_MAX_MILLIS).isGreaterThan(Coordinator.SESSION_TIMEOUT_MILLIS);
    }
}
