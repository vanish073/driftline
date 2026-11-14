package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.sim.kernel.Scheduler;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.LongFunction;

/**
 * At-least-once delivery from a world model to Driftline (DESIGN §2.1): send, and if no 2xx reply arrives, send
 * again on a backoff schedule with the same idempotency key. The world never assumes a message arrives exactly once.
 */
public final class Delivery {

    /** Retry delays after each attempt, in milliseconds: 30 s, 2 min, 10 min, 1 h, 6 h. */
    static final List<Long> BACKOFF = List.of(30_000L, 120_000L, 600_000L, 3_600_000L, 21_600_000L);

    /** Sends one attempt; the attempt replies with an HTTP status, or never. */
    public interface Attempt {
        void send(int attemptNo, IntConsumer reply);
    }

    private final Scheduler scheduler;
    private final String node;
    private final Attempt attempt;
    private final LongFunction<Void> onGiveUp;
    private boolean acked;
    private boolean gaveUp;
    private int attempts;

    Delivery(Scheduler scheduler, String node, Attempt attempt, LongFunction<Void> onGiveUp) {
        this.scheduler = scheduler;
        this.node = node;
        this.attempt = attempt;
        this.onGiveUp = onGiveUp;
    }

    void start() {
        fire();
    }

    private void fire() {
        if (acked) {
            return;
        }
        int attemptNo = ++attempts;
        attempt.send(attemptNo, status -> {
            if (status >= 200 && status < 300) {
                acked = true;
            }
        });
        if (attemptNo <= BACKOFF.size()) {
            scheduler.after(BACKOFF.get(attemptNo - 1), node, this::fire);
        } else {
            scheduler.after(BACKOFF.get(BACKOFF.size() - 1), node, () -> {
                if (!acked) {
                    gaveUp = true;
                    onGiveUp.apply(scheduler.now());
                }
            });
        }
    }

    public boolean acked() {
        return acked;
    }

    public boolean gaveUp() {
        return gaveUp;
    }

    public int attempts() {
        return attempts;
    }
}
