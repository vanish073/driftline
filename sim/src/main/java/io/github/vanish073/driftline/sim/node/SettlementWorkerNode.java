package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.SettlementRepo.Receipt;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.world.Delivery;
import io.github.vanish073.driftline.sim.world.SettlementUploads;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.settlement.MalformedSettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileParser;
import io.github.vanish073.driftline.wire.settlement.SettlementIntake;
import java.time.Instant;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntConsumer;

/**
 * Driftline's settlement worker in the simulator (DESIGN §3.3, §6.2), separate from ingest as the brief asks. For each
 * upload it runs the real strict parser, records the receipt in the Store (the same bytes again are a duplicate; a
 * different file for a date or file id that already has one is REJECTED_CONFLICT and raises a FILE_CONFLICT anomaly),
 * then emits {@code settlement.line} × N and {@code settlement.file_end} to the merchant's partition with
 * {@code eventKey = sha256:lineNo} and {@code sha256:T}, and answers {@code 202}.
 *
 * <p>A duplicate of an accepted upload is emitted again: the worker does not know whether an earlier emission
 * finished, and the matcher's dedupe absorbs the repeats (§6.2: "the worker re-emits everything"). A conflicting
 * upload is recorded and answered {@code 202} too, because it was received and the conflict is now Driftline's to
 * escalate; it is never emitted, and neither is any later copy of the same refused bytes. Like ingest it is stateless between uploads, so a crash only makes it unreachable for a while.
 */
public final class SettlementWorkerNode implements SettlementUploads {

    public static final String NAME = "settlement-worker";

    private final Scheduler scheduler;
    private final SimClock clock;
    private final TreeMap<MerchantId, MerchantContext> merchants;
    private final Store store;
    private final SimLog log;
    private final Coordinator coordinator;
    private final FaultInjector faults;
    private final Trace trace;
    private final TreeSet<String> conflicts = new TreeSet<>();
    /** Runs after an upload is refused as a conflict, whose FILE_CONFLICT the resolver must review (BUG-011). */
    private Runnable afterConflict = () -> { };
    private boolean down;
    private int incarnation = 1;
    private long accepted;
    private long duplicates;
    private long rejected;

    public SettlementWorkerNode(Scheduler scheduler, SimClock clock, TreeMap<MerchantId, MerchantContext> merchants,
                                Store store, SimLog log, Coordinator coordinator, FaultInjector faults, Trace trace) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.merchants = merchants;
        this.store = store;
        this.log = log;
        this.coordinator = coordinator;
        this.faults = faults;
        this.trace = trace;
    }

    public SimClock clock() {
        return clock;
    }

    /**
     * Called after a conflicting upload raised its FILE_CONFLICT. The simulation wakes the resolver here, as the
     * matchers do after a commit: the resolver never polls, and without this a conflict raised when nothing else was
     * happening waited past its deadline (BUG-011). Production's resolver polls every second, so it was not affected.
     */
    public void afterConflict(Runnable hook) {
        this.afterConflict = hook;
    }

    public void crash(long downtimeMillis) {
        if (down) {
            return;
        }
        down = true;
        int dead = ++incarnation;
        faults.count("worker.crashes");
        trace.line(scheduler.now(), NAME, "crashes, down for " + downtimeMillis + " ms");
        scheduler.after(downtimeMillis, NAME, () -> restart(dead));
    }

    public void heal() {
        restart(incarnation);
    }

    private void restart(int deadIncarnation) {
        if (down && deadIncarnation == incarnation) {
            down = false;
            trace.line(scheduler.now(), NAME, "restarts");
        }
    }

    @Override
    public void upload(MerchantId merchantId, byte[] body, IntConsumer reply) {
        if (down) {
            return;
        }
        MerchantContext merchant = merchants.get(merchantId);
        if (merchant == null) {
            reply.accept(404);
            return;
        }
        SettlementFile file;
        try {
            file = SettlementFileParser.parse(merchantId, body);
        } catch (MalformedSettlementFile e) {
            rejected++;
            trace.line(scheduler.now(), NAME, "400 " + e.getMessage());
            reply.accept(400);
            return;
        }
        Sha256 sha = Sha256.of(body);
        Instant now = clock.now();
        Receipt receipt;
        try {
            receipt = store.transact("worker-receive-" + file.fileId(),
                    tx -> SettlementIntake.receive(tx, file, sha, now));
        } catch (StoreUnavailable | StoreConflict e) {
            faults.count("store.retries");
            return;   // no reply: the bank retries on its schedule
        }
        switch (receipt) {
            case CONFLICT -> {
                conflicts.add(merchantId + "/" + file.settlementDate());
                trace.line(scheduler.now(), NAME, "202 CONFLICT " + file.fileId() + " " + sha.hex().substring(0, 12));
                afterConflict.run();
                reply.accept(202);
                return;
            }
            case DUPLICATE -> duplicates++;
            case NEW -> accepted++;
        }
        emit(merchant, file, sha, now);
        boolean ackLost = faults.produceAmbiguous();
        trace.line(scheduler.now(), NAME, (ackLost ? "503 (emitted, ack lost) " : "202 ") + receipt + " "
                + file.fileId() + " lines=" + file.lines().size());
        reply.accept(ackLost ? Delivery.TAKEN_ACK_LOST : 202);
    }

    private void emit(MerchantContext merchant, SettlementFile file, Sha256 sha, Instant receivedAt) {
        for (EventEnvelope envelope : SettlementIntake.envelopes(merchant.partition(), file, sha, receivedAt)) {
            log.append(envelope);
        }
        coordinator.wake(merchant.partition());
    }

    /** {@code merchant/date} of every upload refused as a conflict. */
    public TreeSet<String> conflicts() {
        return new TreeSet<>(conflicts);
    }

    public long accepted() {
        return accepted;
    }

    public long duplicates() {
        return duplicates;
    }

    public long rejected() {
        return rejected;
    }
}
