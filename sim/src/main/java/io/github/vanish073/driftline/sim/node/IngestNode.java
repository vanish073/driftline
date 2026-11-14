package io.github.vanish073.driftline.sim.node;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.sim.kernel.SimClock;
import io.github.vanish073.driftline.sim.kernel.SimLog;
import io.github.vanish073.driftline.sim.kernel.Scheduler;
import io.github.vanish073.driftline.sim.kernel.Trace;
import io.github.vanish073.driftline.sim.world.Ingress;
import io.github.vanish073.driftline.sim.world.Request;
import io.github.vanish073.driftline.wire.ingest.IngestNormaliser;
import io.github.vanish073.driftline.wire.ingest.IngestResult;
import io.github.vanish073.driftline.wire.ingest.MerchantContext;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.IntConsumer;

/**
 * Driftline's ingest in the simulator: the real {@link IngestNormaliser} (signature check on raw bytes, strict
 * parsing, envelope), then an append to the merchant's partition and a {@code 202}. Stateless, like production (D3).
 */
public final class IngestNode implements Ingress {

    private final Scheduler scheduler;
    private final SimClock clock;
    private final TreeMap<MerchantId, MerchantContext> merchants;
    private final SimLog log;
    private final TreeMap<Integer, MatcherNode> matchers;
    private final Trace trace;
    private long accepted;
    private long rejected;

    public IngestNode(Scheduler scheduler, SimClock clock, TreeMap<MerchantId, MerchantContext> merchants, SimLog log,
                      TreeMap<Integer, MatcherNode> matchers, Trace trace) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.merchants = merchants;
        this.log = log;
        this.matchers = matchers;
        this.trace = trace;
    }

    @Override
    public void receive(Request request, IntConsumer reply) {
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
        };
        switch (result) {
            case IngestResult.Accepted a -> {
                var record = log.append(a.envelope());
                accepted++;
                trace.line(scheduler.now(), "ingest", "202 " + a.envelope().eventType().wireName() + " "
                        + a.envelope().eventKey() + " -> p" + record.partition() + "@" + record.offset());
                matchers.get(record.partition()).wake();
                reply.accept(202);
            }
            case IngestResult.Rejected r -> {
                rejected++;
                trace.line(scheduler.now(), "ingest", r.status() + " " + r.reason());
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
