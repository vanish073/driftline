package io.github.vanish073.driftline.sim.kernel;

import java.time.Instant;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * The single-threaded discrete-event kernel (DESIGN §11.1). The queue is ordered by {@code (virtualTime, seq)}, where
 * {@code seq} counts insertions, so equal times run in insertion order and the whole run is a total order.
 * Time skips straight to the next item, so days of simulated time take milliseconds.
 */
public final class Scheduler {

    /** One queued item. */
    private record Item(long time, long seq, String node, Runnable action) {
    }

    private final Instant epoch;
    private final PriorityQueue<Item> queue =
            new PriorityQueue<>(Comparator.comparingLong(Item::time).thenComparingLong(Item::seq));
    private long now;
    private long seq;
    private long steps;
    private String currentNode = "kernel";

    public Scheduler(Instant epoch) {
        this.epoch = epoch;
    }

    /** Virtual time in milliseconds since the epoch. */
    public long now() {
        return now;
    }

    public Instant instant() {
        return epoch.plusMillis(now);
    }

    public Instant epoch() {
        return epoch;
    }

    public long steps() {
        return steps;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /** The node whose item is running, for the trace. */
    public String currentNode() {
        return currentNode;
    }

    public void at(long timeMillis, String node, Runnable action) {
        if (timeMillis < now) {
            throw new IllegalArgumentException("cannot schedule in the past: " + timeMillis + " < " + now);
        }
        queue.add(new Item(timeMillis, seq++, node, action));
    }

    public void after(long delayMillis, String node, Runnable action) {
        at(Math.addExact(now, Math.max(0, delayMillis)), node, action);
    }

    /** Runs the next item. @return false if nothing is queued or the next item is after {@code limitMillis} */
    public boolean step(long limitMillis) {
        Item next = queue.peek();
        if (next == null || next.time() > limitMillis) {
            return false;
        }
        queue.poll();
        now = next.time();
        currentNode = next.node();
        steps++;
        next.action().run();
        return true;
    }

    /**
     * Runs every item due by {@code limitMillis}, then moves the clock to it even if nothing was queued there. The
     * simulator never needs this (time skips to the next item); the sandbox does, to keep a world's clock on real time
     * between the calls Driftline makes to it (D65).
     */
    public void advanceTo(long limitMillis) {
        while (step(limitMillis)) {
            // run what is due
        }
        if (limitMillis > now) {
            now = limitMillis;
        }
    }
}
