package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.sim.fault.FaultInjector;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.world.Delivery;
import io.github.vanish073.driftline.sim.world.Ingress;
import io.github.vanish073.driftline.sim.world.Request;
import io.github.vanish073.driftline.wire.ingest.IngestNormaliser;
import io.github.vanish073.driftline.wire.ingest.IngestResult;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.IntConsumer;

/**
 * Driftline's ingest in the simulator: the real {@link IngestNormaliser} (signature check on raw bytes against this
 * node's clock, strict parsing, envelope), then an append to the merchant's partition and a {@code 202}. Stateless,
 * like production (D3), so a crash only makes it unreachable for a while: requests that arrive while it is down get
 * no reply. Under PRODUCE_AMBIGUOUS the append succeeds but the ack is lost, so it answers {@code 503} and the sender
 * retries, putting a second copy in the log.
 */
public final class IngestNode implements Ingress {

    public static final String NAME = "ingest";

    private final Scheduler scheduler;
    private final SimClock clock;
    private final TreeMap<MerchantId, MerchantContext> merchants;
    private final SimLog log;
    private final Coordinator coordinator;
    private final FaultInjector faults;
    private final Trace trace;
    private boolean down;
    private int incarnation = 1;
    private long accepted;
    private long rejected;

    public IngestNode(Scheduler scheduler, SimClock clock, TreeMap<MerchantId, MerchantContext> merchants, SimLog log,
                      Coordinator coordinator, FaultInjector faults, Trace trace) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.merchants = merchants;
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
        faults.count("ingest.crashes");
        trace.line(scheduler.now(), NAME, "crashes, down for " + downtimeMillis + " ms");
        scheduler.after(downtimeMillis, NAME, () -> restart(dead));
    }

    /** HEAL: back up if down. */
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
    public void receive(Request request, IntConsumer reply) {
        if (down) {
            return;   // connection refused: the sender hears nothing and retries on its schedule
        }
        MerchantContext merchant = merchants.get(request.merchantId());
        if (merchant == null) {
            rejected++;
            reply.accept(404);
            return;
        }
        IngestResult result = switch (request) {
            case Request.Webhook w -> IngestNormaliser.webhook(merchant, w.body(), w.signature(), clock.now(), Optional.empty());
            case Request.OrderEvent o ->
                    IngestNormaliser.orderEvent(merchant, o.body(), o.idempotencyKey(), clock.now(), Optional.empty());
            case Request.Observation o -> IngestNormaliser.observation(merchant, o.observation(), clock.now());
        };
        switch (result) {
            case IngestResult.Accepted a -> {
                var record = log.append(a.envelope());
                accepted++;
                coordinator.wake(record.partition());
                boolean ackLost = faults.produceAmbiguous();
                trace.line(scheduler.now(), NAME, (ackLost ? "503 (append acked late) " : "202 ")
                        + a.envelope().eventType().wireName() + " " + a.envelope().eventKey() + " -> p"
                        + record.partition() + "@" + record.offset());
                reply.accept(ackLost ? Delivery.TAKEN_ACK_LOST : 202);
            }
            case IngestResult.Rejected r -> {
                rejected++;
                trace.line(scheduler.now(), NAME, r.status() + " " + r.reason());
                reply.accept(r.status());
            }
        }
    }

    public long accepted() {
        return accepted;
    }

    public long rejected() {
        return rejected;
    }
}
