package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.sim.kernel.Scheduler;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.LongFunction;

/**
 * At-least-once delivery from a world model to Driftline (DESIGN §2.1): send, and if no 2xx reply arrives, send
 * again on a backoff schedule with the same idempotency key. The world never assumes a message arrives once.
 *
 * <p>The schedule is 30 s, 2 min, 10 min, 1 h and 6 h, then every 6 h until {@link #HORIZON_MILLIS} (3 days, as
 * public gateways do) has passed, and then the sender gives up. A message counts as drained once Driftline has
 * taken it, whether or not its answer got back to the sender (D87): Driftline answered an attempt with a 2xx, or with
 * {@link #TAKEN_ACK_LOST}, its answer when the append succeeded but the log's ack was lost (D90). With more traffic days than the horizon ({@code nightly}: 5), a sender can give up inside the faulty phase
 * on a message Driftline took days earlier, every answer lost on the way back. A message Driftline never accepted is
 * a failure to drain, given up on or not, unless the world first sent it within {@link #DRAIN_GRACE_MILLIS} of the
 * end of the run: the bank's feed misbehaves after HEAL too, and a settlement file held back by FILE_LATE can be
 * released moments before the end (seed 209053576945360396, 88 ms before it).
 */
public final class Delivery {

    /** Retry delays after each attempt, in milliseconds: 30 s, 2 min, 10 min, 1 h, 6 h. */
    static final List<Long> BACKOFF = List.of(30_000L, 120_000L, 600_000L, 3_600_000L, 21_600_000L);
    /**
     * What the simulated ingest and settlement worker answer when they took a message but the log's ack was lost
     * (PRODUCE_AMBIGUOUS): the append or emit succeeded, and the sender retries. They give it in no other case (D90).
     */
    public static final int TAKEN_ACK_LOST = 503;
    /** How long a sender keeps retrying, measured from the first attempt. */
    public static final long HORIZON_MILLIS = 3 * 86_400_000L;
    /**
     * How long before the end of a run a message must first have been sent to be judged by INV-DRAINED (D87). After
     * HEAL the network neither drops nor holds messages (one way: at most {@code SimNetwork.MAX_DELAY_MILLIS}, 200 ms),
     * so a message Driftline takes is taken within a round trip of its first attempt, or at its first or second retry
     * (30 s and 2.5 min later) if it was answered 503 first. Ten minutes covers that with room to spare.
     */
    public static final long DRAIN_GRACE_MILLIS = 10 * 60_000L;

    /**
     * Sends one attempt. Driftline's HTTP status goes to {@code answered} where Driftline gives it, and to
     * {@code reply} if it gets back to the sender; either may never happen.
     */
    public interface Attempt {
        void send(int attemptNo, IntConsumer answered, IntConsumer reply);
    }

    private final Scheduler scheduler;
    private final String node;
    private final Attempt attempt;
    private final LongFunction<Void> onGiveUp;
    private Runnable onAck = () -> { };
    private boolean acked;
    private boolean accepted;
    private boolean gaveUp;
    private int attempts;
    private long firstAttemptAt = -1;

    Delivery(Scheduler scheduler, String node, Attempt attempt, LongFunction<Void> onGiveUp) {
        this.scheduler = scheduler;
        this.node = node;
        this.attempt = attempt;
        this.onGiveUp = onGiveUp;
    }

    void start() {
        fire();
    }

    /** Runs {@code action} once, when the first 2xx reply arrives. Set it before {@link #start()}. */
    Delivery onAck(Runnable action) {
        this.onAck = action;
        return this;
    }

    private void fire() {
        if (acked) {
            return;
        }
        int attemptNo = ++attempts;
        if (firstAttemptAt < 0) {
            firstAttemptAt = scheduler.now();
        }
        attempt.send(attemptNo, status -> {
            if (status >= 200 && status < 300 || status == TAKEN_ACK_LOST) {
                accepted = true;
            }
        }, status -> {
            if (status >= 200 && status < 300 && !acked) {
                acked = true;
                onAck.run();
            }
        });
        long wait = BACKOFF.get(Math.min(attemptNo, BACKOFF.size()) - 1);
        if (scheduler.now() + wait - firstAttemptAt <= HORIZON_MILLIS) {
            scheduler.after(wait, node, this::fire);
        } else {
            scheduler.after(wait, node, () -> {
                if (!acked) {
                    gaveUp = true;
                    onGiveUp.apply(scheduler.now());
                }
            });
        }
    }

    /** Whether a 2xx reached the sender. */
    public boolean acked() {
        return acked;
    }

    /**
     * Whether Driftline took the message: it answered an attempt with a 2xx or {@link #TAKEN_ACK_LOST}, whether or not
     * the answer reached the sender (D87, D90).
     */
    public boolean accepted() {
        return accepted;
    }

    /** A failure to drain at the end of a run (D87): never accepted, though first sent before {@code judgedBefore}. */
    public boolean undrained(long judgedBefore) {
        return !accepted && firstAttemptAt >= 0 && firstAttemptAt < judgedBefore;
    }

    public boolean gaveUp() {
        return gaveUp;
    }

    public int attempts() {
        return attempts;
    }
}
