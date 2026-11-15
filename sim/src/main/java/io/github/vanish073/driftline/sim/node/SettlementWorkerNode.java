package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.SettlementRepo.Receipt;
import io.github.vanish073.driftline.core.port.SettlementRepo.ReceiptStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.ReceivedFile;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.port.StoreConflict;
import io.github.vanish073.driftline.core.port.StoreUnavailable;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.world.SettlementUploads;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import io.github.vanish073.driftline.wire.settlement.MalformedSettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileParser;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
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
            receipt = store.transact("worker-receive-" + file.fileId(), tx -> {
                Receipt r = tx.settlement().receive(new ReceivedFile(merchantId, file.fileId(), file.settlementDate(),
                        sha, file.lines().size(), file.totalAmount(), now, ReceiptStatus.ACCEPTED));
                if (r == Receipt.DUPLICATE
                        && tx.settlement().receipt(sha).orElseThrow().status() == ReceiptStatus.REJECTED_CONFLICT) {
                    return Receipt.CONFLICT;   // the same refused bytes again: still refused, never emitted (BUG-002)
                }
                if (r == Receipt.CONFLICT) {
                    TreeMap<String, String> facts = new TreeMap<>();
                    facts.put("file_id", file.fileId());
                    facts.put("settlement_date", file.settlementDate().toString());
                    facts.put("sha256", sha.hex());
                    tx.breaks().raise(Break.detected(merchantId, BreakType.FILE_CONFLICT,
                            file.settlementDate() + ":" + sha.hex().substring(0, 16), now, facts), "upload refused");
                }
                return r;
            });
        } catch (StoreUnavailable | StoreConflict e) {
            faults.count("store.retries");
            return;   // no reply: the bank retries on its schedule
        }
        switch (receipt) {
            case CONFLICT -> {
                conflicts.add(merchantId + "/" + file.settlementDate());
                trace.line(scheduler.now(), NAME, "202 CONFLICT " + file.fileId() + " " + sha.hex().substring(0, 12));
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
        reply.accept(ackLost ? 503 : 202);
    }

    private void emit(MerchantContext merchant, SettlementFile file, Sha256 sha, Instant receivedAt) {
        int partition = merchant.partition();
        for (SettlementLinePayload line : file.lines()) {
            String key = sha.hex() + ":" + line.lineNo();
            log.append(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, merchant.merchantId(), partition, Source.BANK,
                    key, EventType.SETTLEMENT_LINE, file.generatedAt(), receivedAt,
                    Sha256.of(key.getBytes(StandardCharsets.US_ASCII)), line, Optional.empty()));
        }
        String endKey = sha.hex() + ":T";
        log.append(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, merchant.merchantId(), partition, Source.BANK,
                endKey, EventType.SETTLEMENT_FILE_END, file.generatedAt(), receivedAt,
                Sha256.of(endKey.getBytes(StandardCharsets.US_ASCII)), file.fileEnd(sha), Optional.empty()));
        coordinator.wake(partition);
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
