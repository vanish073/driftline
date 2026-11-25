package io.github.vanish073.driftline.app.pii;

import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.CustomerVault;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Deletes customers' contacts once they are older than the retention period (Shift 11, D99), every {@code interval},
 * in batches of {@link #BATCH}, in the resolver role (one place per deployment, like the book metrics). Each deletion
 * leaves a tombstone, so a later webhook for the payment cannot store the contact again.
 */
public final class ContactRetention implements SmartLifecycle {

    static final int BATCH = 1_000;
    private static final Logger LOG = LoggerFactory.getLogger(ContactRetention.class);

    private final CustomerVault vault;
    private final Clock clock;
    private final Duration retention;
    private final Duration interval;
    private volatile boolean running;
    private volatile Thread thread;

    public ContactRetention(CustomerVault vault, Clock clock, Duration retention, Duration interval) {
        this.vault = Objects.requireNonNull(vault, "vault");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (retention.isNegative() || retention.isZero() || interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("the retention period and the purge interval must be positive");
        }
        this.retention = retention;
        this.interval = interval;
    }

    /** One pass: deletes every contact older than the retention period. Public for tests. */
    public int purge() {
        Instant now = clock.now();
        int total = 0;
        int n;
        do {
            n = vault.purgeOlderThan(now.minus(retention), BATCH, now);
            total += n;
        } while (n == BATCH);
        if (total > 0) {
            LOG.info("deleted {} customer contacts older than {} (retention, D99)", total, retention);
        }
        return total;
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            thread = Thread.ofPlatform().name("contact-retention").daemon().start(this::run);
        }
    }

    private void run() {
        while (running) {
            try {
                purge();
            } catch (RuntimeException e) {
                LOG.warn("customer contacts not purged this time: {}", e.toString());
            }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
