package io.github.vanish073.driftline.sim.random;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * The simulator's only source of randomness (DESIGN §11.2, D13): SplitMix64, written here so a seed reproduces across
 * JDK versions. Independent streams for the workload, faults, network and swarm are derived as
 * {@code SHA-256(seed || label)}, so turning a fault off never disturbs the workload.
 *
 * <p>Integer arithmetic only: no floating point, so results cannot vary by platform.
 */
public final class SplitMix64 {

    private long state;

    public SplitMix64(long seed) {
        this.state = seed;
    }

    /** The stream for {@code label}: seeded with the first 8 bytes of SHA-256(seed big-endian || label in UTF-8). */
    public static SplitMix64 stream(long seed, String label) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(ByteBuffer.allocate(Long.BYTES).putLong(seed).array());
            sha256.update(label.getBytes(StandardCharsets.UTF_8));
            return new SplitMix64(ByteBuffer.wrap(sha256.digest()).getLong());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("every Java platform must provide SHA-256", impossible);
        }
    }

    public long nextLong() {
        long z = (state += 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Uniform in {@code [0, bound)}, without modulo bias. */
    public long nextLong(long bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive, got " + bound);
        }
        while (true) {
            long bits = nextLong() >>> 1;
            long value = bits % bound;
            if (bits - value + (bound - 1) >= 0) {
                return value;
            }
        }
    }

    /** Uniform in {@code [low, high]}. */
    public long between(long low, long high) {
        if (high < low) {
            throw new IllegalArgumentException("empty range [" + low + ", " + high + "]");
        }
        return low + nextLong(Math.addExact(Math.subtractExact(high, low), 1));
    }

    public int between(int low, int high) {
        return Math.toIntExact(between((long) low, (long) high));
    }

    /** True with probability {@code numerator / denominator}. */
    public boolean chance(long numerator, long denominator) {
        if (numerator < 0 || denominator <= 0 || numerator > denominator) {
            throw new IllegalArgumentException("not a probability: " + numerator + "/" + denominator);
        }
        return nextLong(denominator) < numerator;
    }

    public <T> T pick(List<T> items) {
        if (items.isEmpty()) {
            throw new IllegalArgumentException("cannot pick from an empty list");
        }
        return items.get(Math.toIntExact(nextLong(items.size())));
    }
}
