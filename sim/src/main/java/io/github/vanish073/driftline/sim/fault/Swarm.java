package io.github.vanish073.driftline.sim.fault;

import io.github.vanish073.driftline.sim.random.SplitMix64;
import java.util.EnumMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Which faults one seed switches on, and how hard (DESIGN §11.5, swarm testing): each fault is on with probability
 * 1/2, so most seeds run a different mix rather than everything at once. A rate fault's intensity is a probability in
 * permille; an event fault's intensity is how many times it strikes per traffic day. The vector is printed in the
 * trace header.
 */
public final class Swarm {

    /** No faults: the {@code calm} profile. */
    public static final Swarm NONE = new Swarm(new EnumMap<>(Fault.class));

    private final EnumMap<Fault, Integer> intensity;

    private Swarm(EnumMap<Fault, Integer> intensity) {
        this.intensity = intensity;
    }

    /** Draws the vector from the {@code swarm} stream. Every fault consumes the same draws whether it is on or not. */
    public static Swarm draw(SplitMix64 rng) {
        EnumMap<Fault, Integer> on = new EnumMap<>(Fault.class);
        for (Fault f : Fault.values()) {
            boolean enabled = rng.chance(1, 2);
            int level = Math.toIntExact(switch (f) {
                case NET_DROP -> rng.between(10, 150);          // 1% to 15% of messages
                case NET_DUP -> rng.between(10, 150);
                case NET_DELAY -> rng.between(20, 300);
                case PRODUCE_AMBIGUOUS -> rng.between(10, 150);
                case MERCHANT_KEY_REUSE -> rng.between(20, 200);
                case FILE_LATE -> rng.between(100, 500);                 // 10% to 50% of files
                case FILE_DUP, FILE_CONFLICT -> rng.between(50, 300);
                case CALL_AMBIGUOUS -> rng.between(20, 200);
                case WEBHOOK_LOST -> rng.between(5, 50);
                case STATUS_STALE -> rng.between(50, 400);
                case NODE_CRASH, NODE_PAUSE, CLOCK_JUMP, STORE_DOWN -> rng.between(1, 4);   // per traffic day
                case CLOCK_SKEW -> rng.between(1, 1);
            });
            if (enabled) {
                on.put(f, level);
            }
        }
        return new Swarm(on);
    }

    /** A vector with exactly these faults on, for tests. */
    public static Swarm of(Map<Fault, Integer> faults) {
        return new Swarm(new EnumMap<>(faults));
    }

    public boolean on(Fault f) {
        return intensity.containsKey(f);
    }

    /** The fault's intensity, or 0 when it is off. */
    public int level(Fault f) {
        return intensity.getOrDefault(f, 0);
    }

    public boolean isNone() {
        return intensity.isEmpty();
    }

    /** For example {@code NET_DROP=37‰ NODE_PAUSE=2/day}, or {@code none}. */
    @Override
    public String toString() {
        if (intensity.isEmpty()) {
            return "none";
        }
        StringJoiner out = new StringJoiner(" ");
        intensity.forEach((f, level) -> out.add(f.name() + "=" + level + (f.isRate() ? "‰" : f == Fault.CLOCK_SKEW ? "" : "/day")));
        return out.toString();
    }
}
