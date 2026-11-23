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
 * answered one of its attempts with a 2xx, whether or not that answer got back to the sender (D87): Driftline has then
 * taken it. With more traffic days than the horizon ({@code nightly}: 5), a sender can give up inside the faulty phase
 * on a message Driftline took days earlier, every answer lost on the way back. A message Driftline never accepted is
 * a failure to drain, given up on or not.
 */
public final class Delivery {

    /** Retry delays after each attempt, in milliseconds: 30 s, 2 min, 10 min, 1 h, 6 h. */
    static final List<Long> BACKOFF = List.of(30_000L, 120_000L, 600_000L, 3_600_000L, 21_600_000L);
    /** How long a sender keeps retrying, measured from the first attempt. */
    public static final long HORIZON_MILLIS = 3 * 86_400_000L;

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
            if (status >= 200 && status < 300) {
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

    /** Whether Driftline answered an attempt with a 2xx, whether or not the answer reached the sender (D87). */
    public boolean accepted() {
        return accepted;
    }

    public boolean gaveUp() {
        return gaveUp;
    }

    public int attempts() {
        return attempts;
    }
}
