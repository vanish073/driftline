package io.github.vanish073.driftline.app.merchant;

import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Reloads the merchants' active webhook secrets every {@code interval} (D76), so a rotated secret is accepted, and a
 * retired one refused, on every instance within one interval, without a restart. A failed reload is logged and the
 * previous secrets stay in force until the next one.
 */
public final class SecretRefresher implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SecretRefresher.class);

    private final MerchantRegistry registry;
    private final PostgresDirectory directory;
    private final SecretCipher cipher;
    private final Clock clock;
    private final Duration interval;
    private volatile ScheduledExecutorService executor;

    public SecretRefresher(MerchantRegistry registry, PostgresDirectory directory, SecretCipher cipher, Clock clock,
                           Duration interval) {
        this.registry = registry;
        this.directory = directory;
        this.cipher = cipher;
        this.clock = clock;
        this.interval = interval;
    }

    /** One reload, now. */
    public void refresh() {
        try {
            int kept = registry.refreshSecrets(directory, cipher, clock.now());
            if (kept > 0) {
                LOG.error("{} merchants have no valid set of active webhook secrets; their previous ones stay in force",
                        kept);
            }
        } catch (RuntimeException e) {
            LOG.warn("webhook secret reload failed; the previous secrets stay in force: {}", e.toString());
        }
    }

    @Override
    public synchronized void start() {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "secret-refresher");
                t.setDaemon(true);
                return t;
            });
            executor.scheduleWithFixedDelay(this::refresh, interval.toMillis(), interval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
