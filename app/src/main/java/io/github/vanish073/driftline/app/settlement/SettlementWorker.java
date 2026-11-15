package io.github.vanish073.driftline.app.settlement;

import io.github.vanish073.driftline.app.kafka.EventPublisher;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.SettlementRepo.Receipt;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.settlement.MalformedSettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileParser;
import io.github.vanish073.driftline.wire.settlement.SettlementIntake;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The separate settlement worker (the brief: "settlement files are parsed by a separate worker"; DESIGN §3.3, §6.2,
 * D48). It takes uploads the upload endpoint left in the {@link SettlementInbox}, and for each one:
 * <ol>
 *   <li>parses the raw bytes with the strict parser (a refusal is recorded as REJECTED_MALFORMED);</li>
 *   <li>records the receipt in one Store transaction with {@link SettlementIntake#receive}, the same code the
 *       simulator runs: a conflicting file is refused and escalated as FILE_CONFLICT, never emitted;</li>
 *   <li>emits {@code settlement.line} × N and {@code settlement.file_end} to the merchant's partition and waits for
 *       Kafka's acknowledgement of every record;</li>
 *   <li>marks the upload EMITTED.</li>
 * </ol>
 * A crash between any two steps leaves the upload RECEIVED, so it is done again: the receipt comes back DUPLICATE, the
 * records are emitted again and the matcher's dedupe absorbs them (CP8: "re-emitted; deduped; file COMPLETE exactly
 * once"). Crash point {@code CP8} sits between the emit and the final mark.
 */
public final class SettlementWorker implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SettlementWorker.class);

    private final SettlementInbox inbox;
    private final Store store;
    private final EventPublisher publisher;
    private final MerchantRegistry merchants;
    private final Clock clock;
    private final CrashPoints crashPoints;
    private final Duration pollInterval;
    private final int batchSize;

    private volatile boolean running;
    private volatile Thread thread;
    private volatile Throwable failure;

    public SettlementWorker(SettlementInbox inbox, Store store, EventPublisher publisher, MerchantRegistry merchants,
                            Clock clock, CrashPoints crashPoints, Duration pollInterval, int batchSize) {
        this.inbox = Objects.requireNonNull(inbox, "inbox");
        this.store = Objects.requireNonNull(store, "store");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.merchants = Objects.requireNonNull(merchants, "merchants");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.crashPoints = Objects.requireNonNull(crashPoints, "crashPoints");
        this.pollInterval = pollInterval;
        this.batchSize = batchSize;
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            failure = null;
            thread = Thread.ofPlatform().name("settlement-worker").start(this::run);
        }
    }

    @Override
    public void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        Thread t = thread;
        return running && t != null && t.isAlive();
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure);
    }

    private void run() {
        try {
            while (running) {
                int done = drainOnce();
                if (done == 0) {
                    try {
                        Thread.sleep(pollInterval);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            running = false;
            LOG.error("settlement worker stopped; the process must restart and will redo unfinished uploads", e);
        }
    }

    /** Processes up to one batch of pending uploads; returns how many it finished. Public for tests. */
    public int drainOnce() {
        List<SettlementInbox.Upload> pending;
        try {
            pending = inbox.pending(batchSize);
        } catch (StoreUnavailable e) {
            return 0;
        }
        int done = 0;
        for (SettlementInbox.Upload upload : pending) {
            try {
                if (handle(upload)) {
                    done++;
                }
            } catch (StoreUnavailable | StoreConflict | EventPublisher.EventLogUnavailable e) {
                LOG.warn("settlement upload {} for {} will be retried: {}", upload.sha256().hex(), upload.merchantId(),
                        e.getMessage());
                return done;
            }
        }
        return done;
    }

    private boolean handle(SettlementInbox.Upload upload) {
        Instant now = clock.now();
        Optional<MerchantContext> merchant = merchants.context(upload.merchantId());
        if (merchant.isEmpty()) {
            LOG.warn("settlement upload for merchant {} which this process does not know yet; left for a restart",
                    upload.merchantId());
            return false;
        }
        SettlementFile file;
        try {
            file = SettlementFileParser.parse(upload.merchantId(), upload.raw());
        } catch (MalformedSettlementFile e) {
            String error = e.getMessage() == null ? "malformed" : e.getMessage();
            return inbox.finish(upload.merchantId(), upload.sha256(), SettlementInbox.State.REJECTED_MALFORMED,
                    Optional.of(error.length() > 300 ? error.substring(0, 300) : error), now);
        }
        Receipt receipt = store.transact("worker-receive-" + file.fileId(),
                tx -> SettlementIntake.receive(tx, file, upload.sha256(), upload.receivedAt()));
        if (receipt == Receipt.CONFLICT) {
            LOG.warn("settlement file {} for {} refused as a conflict (FILE_CONFLICT raised)", file.fileId(),
                    upload.merchantId());
            return inbox.finish(upload.merchantId(), upload.sha256(), SettlementInbox.State.REFUSED_CONFLICT,
                    Optional.empty(), now);
        }
        publisher.publish(SettlementIntake.envelopes(merchant.get().partition(), file, upload.sha256(),
                upload.receivedAt()));
        crashPoints.reached(CrashPoint.CP8);
        boolean moved = inbox.finish(upload.merchantId(), upload.sha256(), SettlementInbox.State.EMITTED,
                Optional.empty(), clock.now());
        LOG.info("settlement file {} for {}: {} lines emitted ({})", file.fileId(), upload.merchantId(),
                file.lines().size(), receipt);
        return moved;
    }
}
