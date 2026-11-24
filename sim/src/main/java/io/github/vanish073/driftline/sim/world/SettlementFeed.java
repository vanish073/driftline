package io.github.vanish073.driftline.sim.world;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimNetwork;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * The bank's settlement feed (DESIGN §11.4): each file is written in Driftline Settlement File v1 with the real
 * {@link SettlementFileWriter} and uploaded at least once, like every other world message. The settlement-file faults
 * strike here: FILE_LATE holds a file back, FILE_DUP uploads the same bytes again, and FILE_CONFLICT uploads a
 * re-generated file for the same date once the original was accepted (so the original always wins, as in §6.2).
 */
public final class SettlementFeed {

    public static final String NODE = "bank";
    public static final String WORKER = "settlement-worker";
    static final long DUPLICATE_AFTER_MILLIS = 3_600_000L;
    static final long CONFLICT_AFTER_MILLIS = 2 * 3_600_000L;

    private final Scheduler scheduler;
    private final SimNetwork network;
    private final FaultInjector faults;
    private final Trace trace;
    private final SettlementUploads worker;
    private final List<Delivery> deliveries = new ArrayList<>();
    private final TreeSet<String> conflicting = new TreeSet<>();
    private long files;

    public SettlementFeed(Scheduler scheduler, SimNetwork network, FaultInjector faults, Trace trace,
                          SettlementUploads worker) {
        this.scheduler = scheduler;
        this.network = network;
        this.faults = faults;
        this.trace = trace;
        this.worker = worker;
    }

    void deliver(SettlementFile file) {
        files++;
        byte[] body = SettlementFileWriter.write(file);
        long delay = faults.fileDelay();
        boolean duplicate = faults.fileDuplicate();
        boolean conflict = faults.fileConflict();
        MerchantId m = file.merchantId();
        trace.line(scheduler.now(), NODE, "file " + file.fileId() + " lines=" + file.lines().size() + " total="
                + file.totalAmount() + (delay > 0 ? " LATE by " + delay + " ms" : "") + (duplicate ? " DUP" : "")
                + (conflict ? " CONFLICT" : ""));
        scheduler.after(delay, NODE, () -> send(m, body, () -> {
            if (duplicate) {
                scheduler.after(DUPLICATE_AFTER_MILLIS, NODE, () -> send(m, body, () -> { }));
            }
            if (conflict) {
                SettlementFile regenerated = new SettlementFile(file.fileId(), m, file.settlementDate(),
                        file.generatedAt().plusSeconds(3_600), file.lines(), file.totalAmount());
                conflicting.add(m + "/" + file.settlementDate());
                scheduler.after(CONFLICT_AFTER_MILLIS, NODE, () -> send(m, SettlementFileWriter.write(regenerated),
                        () -> { }));
            }
        }));
    }

    private void send(MerchantId m, byte[] body, Runnable onAck) {
        Delivery delivery = new Delivery(scheduler, NODE, (attemptNo, answered, reply) -> network.send(WORKER,
                () -> worker.upload(m, body, status -> {
                    answered.accept(status);
                    network.send(NODE, () -> reply.accept(status));
                })), at -> null).onAck(onAck);
        deliveries.add(delivery);
        delivery.start();
    }

    /** {@code merchant/date} of every date for which a conflicting file was sent. */
    public TreeSet<String> conflicting() {
        return new TreeSet<>(conflicting);
    }

    public long files() {
        return files;
    }

    /** Uploads the worker never accepted though first sent before {@code judgedBefore} (D87), given up on or not. */
    public long undelivered(long judgedBefore) {
        return deliveries.stream().filter(d -> d.undrained(judgedBefore)).count();
    }
}
