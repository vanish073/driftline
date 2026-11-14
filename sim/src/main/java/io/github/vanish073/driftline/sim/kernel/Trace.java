package io.github.vanish073.driftline.sim.kernel;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/**
 * The run's timeline. Every line is folded into a SHA-256 digest; two runs of one seed must give the same digest
 * (INV-DETERMINISTIC, §11.7). The last {@value #TAIL} lines are kept for failure reports (§11.12); every line is kept
 * only when asked for.
 */
public final class Trace {

    public static final int TAIL = 200;

    private final MessageDigest digest;
    private final ArrayDeque<String> tail = new ArrayDeque<>();
    private final List<String> all;
    private long lines;

    public Trace(boolean keepAll) {
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("every Java platform must provide SHA-256", impossible);
        }
        this.all = keepAll ? new ArrayList<>() : null;
    }

    public void line(long timeMillis, String node, String text) {
        String line = pad(timeMillis) + " " + node + " " + text;
        digest.update(line.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
        tail.addLast(line);
        if (tail.size() > TAIL) {
            tail.removeFirst();
        }
        if (all != null) {
            all.add(line);
        }
        lines++;
    }

    /** Milliseconds of virtual time, zero-padded to 12 digits so lines sort and align. */
    private static String pad(long millis) {
        String digits = Long.toString(millis);
        return "0".repeat(Math.max(0, 12 - digits.length())) + digits;
    }

    public long lines() {
        return lines;
    }

    public List<String> tail() {
        return List.copyOf(tail);
    }

    /** Every line, if this trace keeps them all; otherwise empty. */
    public List<String> all() {
        return all == null ? List.of() : List.copyOf(all);
    }

    /** The digest so far, as hex. Reading it does not disturb later lines. */
    public String digestHex() {
        try {
            return HexFormat.of().formatHex(((MessageDigest) digest.clone()).digest());
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("SHA-256 digests are cloneable on every Java platform", e);
        }
    }
}
