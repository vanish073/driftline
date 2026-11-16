package io.github.vanish073.driftline.sandbox;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.world.Scenario;
import io.github.vanish073.driftline.wire.call.CallWire;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The sandbox over the JDK HttpServer, no Spring (D20, D51, D65). Requests are handled one at a time, and every world
 * is touched only under this object's lock.
 *
 * <ul>
 *   <li><b>Control API.</b> {@code POST /v1/inject?type=DUPLICATE_CAPTURE&count=5} takes the next unused merchant of
 *       the bootstrap pool, plays the scenario with the simulator's world models, and answers with what Driftline must
 *       now show; {@code type=AMBIGUOUS_BATCH} plants unreferenced lines that batch matching must report as ambiguous
 *       (D68). {@code GET /v1/stats} reports what the worlds did to Driftline's requests: refunds executed, payments
 *       refunded more than once, escalations received. {@code GET /health} answers {@code ok}.</li>
 *   <li><b>The world's own APIs</b>, which Driftline's dispatcher calls (shapes in {@link CallWire}): the gateway under
 *       {@code /gateway} (status, order status, refund, refund lookup), the merchant's confirm callback under
 *       {@code /merchant}, and the ops escalation sink under {@code /ops}. Each injected world stays alive for them; its
 *       clock is brought to real time before each call and by a ticker every {@value #TICK_MILLIS} ms, which also
 *       delivers what a call set off (a refund webhook, a merchant's {@code order.paid}).</li>
 * </ul>
 * Every fault stays off (D51): no ambiguous calls, stale statuses or lost webhooks. The gateway forgets an idempotency
 * key after {@code FakeGateway.KEY_TTL_MILLIS} (5 min), so Driftline's dispatcher must be configured with that TTL.
 */
public final class SandboxServer {

    static final long TICK_MILLIS = 200;
    /** The ambiguous-line injection (D68): equal-priced payments paid in two unreferenced lines. */
    static final String AMBIGUOUS_BATCH = "AMBIGUOUS_BATCH";
    static final int MAX_BODY = 64 * 1024;

    private final HttpServer server;
    private final URI driftline;
    private final BootstrapFile.Pool pool;
    private final Deque<BootstrapFile.Merchant> unused;
    private final TreeMap<MerchantId, Injection.World> worlds = new TreeMap<>();
    /** Escalation notices received, by break id (a repeated delivery is one notice). */
    private final TreeMap<String, String> escalations = new TreeMap<>();
    private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "sandbox-ticker");
        t.setDaemon(true);
        return t;
    });
    private long calls;

    public SandboxServer(int port, URI driftline, BootstrapFile.Pool pool) throws IOException {
        this.driftline = driftline;
        this.pool = pool;
        this.unused = new ArrayDeque<>(pool.merchants());
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> respond(exchange, 200, "ok\n"));
        server.createContext("/v1/inject", this::inject);
        server.createContext("/v1/stats", this::stats);
        server.createContext("/gateway", exchange -> call(exchange, CallTarget.GATEWAY, "/gateway"));
        server.createContext("/merchant", exchange -> call(exchange, CallTarget.MERCHANT, "/merchant"));
        server.createContext("/ops", exchange -> call(exchange, CallTarget.OPS, "/ops"));
    }

    public void start() {
        server.start();
        ticker.scheduleWithFixedDelay(this::tick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        ticker.shutdownNow();
        server.stop(0);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private synchronized void tick() {
        Instant now = Instant.now();
        try {
            worlds.values().forEach(w -> w.advanceTo(now));
        } catch (RuntimeException e) {
            System.err.println("sandbox ticker: " + e);   // a failed delivery is retried by the world's own rules
        }
    }

    private synchronized void inject(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, json("error", "POST only"));
            return;
        }
        Map<String, String> query = query(exchange.getRequestURI());
        String type = query.getOrDefault("type", "").toUpperCase(Locale.ROOT);
        boolean ambiguousBatch = type.equals(AMBIGUOUS_BATCH);
        Scenario scenario;
        int count;
        try {
            scenario = ambiguousBatch ? Scenario.NORMAL : Scenario.valueOf(type);
            count = Integer.parseInt(query.getOrDefault("count", "1"));
        } catch (IllegalArgumentException e) {
            respond(exchange, 400, json("error", "type must be a scenario or " + AMBIGUOUS_BATCH
                    + ", and count a number"));
            return;
        }
        if (!ambiguousBatch && !Injection.oneToOne().contains(scenario) && scenario != Scenario.NORMAL) {
            respond(exchange, 400, json("error", "type must be NORMAL, a one-to-one break scenario or "
                    + AMBIGUOUS_BATCH));
            return;
        }
        BootstrapFile.Merchant merchant = unused.pollFirst();
        if (merchant == null) {
            respond(exchange, 409, json("error", "every merchant in the pool has been used; restart with a new pool"));
            return;
        }
        try {
            HttpDriftline target = new HttpDriftline(driftline, pool.merchants());
            LocalDate today = IstCalendar.istDate(Instant.now());
            Injection.Report r = ambiguousBatch ? Injection.ambiguousBatch(merchant, count, target, today)
                    : Injection.run(merchant, scenario, count, target, today);
            worlds.put(r.merchantId(), r.world());
            respond(exchange, 200, new String(StrictJson.write(g -> {
                g.writeStringProperty("merchant_id", r.merchantId().value());
                g.writeNumberProperty("partition", merchant.partition());
                g.writeStringProperty("type", ambiguousBatch ? AMBIGUOUS_BATCH : r.scenario().name());
                g.writeNumberProperty("count", r.count());
                g.writeStringProperty("last_file_date", r.lastFileDate().toString());
                g.writeNumberProperty("expected_count", r.expected().size());
                g.writeStringProperty("expected_breaks", String.join(";", r.expected()));
                g.writeNumberProperty("webhooks", r.webhooks());
                g.writeNumberProperty("order_events", r.orderEvents());
                g.writeNumberProperty("settlement_files", r.files());
                g.writeNumberProperty("not_accepted", r.notAccepted());
                g.writeStringProperty("batch_lines", String.join(";", r.batchLines()));
            }), StandardCharsets.UTF_8) + "\n");
        } catch (RuntimeException e) {
            respond(exchange, 500, json("error", e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    /**
     * {@code GET /v1/stats[?merchant=m]}: totals over every world (or one merchant's), for {@code driftline verify} and
     * the tests. A payment refunded more than once is exactly what INV-REFUND-ONCE forbids.
     */
    private synchronized void stats(HttpExchange exchange) throws IOException {
        Optional<String> only = Optional.ofNullable(query(exchange.getRequestURI()).get("merchant"));
        long refunds = 0;
        TreeMap<String, Long> perPayment = new TreeMap<>();
        for (Injection.World w : worlds.values()) {
            if (only.isPresent() && !only.get().equals(w.merchantId().value())) {
                continue;
            }
            for (PaymentId p : w.gateway().refundLog()) {
                refunds++;
                perPayment.merge(w.merchantId().value() + "/" + p.value(), 1L, Long::sum);
            }
        }
        long total = refunds;
        long twice = perPayment.values().stream().filter(n -> n > 1).count();
        respond(exchange, 200, new String(StrictJson.write(g -> {
            g.writeNumberProperty("worlds", worlds.size());
            g.writeNumberProperty("refunds", total);
            g.writeNumberProperty("payments_refunded", perPayment.size());
            g.writeNumberProperty("payments_refunded_more_than_once", twice);
            g.writeNumberProperty("escalations", escalations.size());
        }), StandardCharsets.UTF_8) + "\n");
    }

    /** One dispatcher call to the gateway, the merchant or the ops sink (D65). */
    private synchronized void call(HttpExchange exchange, CallTarget target, String prefix) throws IOException {
        byte[] body;
        try (InputStream in = exchange.getRequestBody()) {
            body = in.readNBytes(MAX_BODY + 1);
        }
        if (body.length > MAX_BODY) {
            respond(exchange, 413, json("error", "body too large"));
            return;
        }
        String path = exchange.getRequestURI().getRawPath().substring(prefix.length());
        Optional<CallWire.Incoming> incoming;
        try {
            incoming = CallWire.parse(target, exchange.getRequestMethod(), path, body);
        } catch (MalformedJson e) {
            respond(exchange, 400, json("error", e.getMessage()));
            return;
        }
        if (incoming.isEmpty()) {
            respond(exchange, 404, json("error", "no such route"));
            return;
        }
        MerchantId m = incoming.get().merchantId();
        CallRequest request = incoming.get().request();
        Optional<String> key = Optional.ofNullable(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
        if (request instanceof CallRequest.EscalationNotify e) {
            escalations.putIfAbsent(e.breakId(), e.type() + " " + e.subject());
            reply(exchange, CallWire.encodeReply(request, Map.of("status", "DELIVERED")));
            return;
        }
        Injection.World world = worlds.get(m);
        if (world == null) {
            respond(exchange, 404, json("error", "no world for that merchant"));
            return;
        }
        world.advanceTo(Instant.now());
        TreeMap<String, String> facts = new TreeMap<>();
        if (target == CallTarget.MERCHANT) {
            facts.putAll(world.merchant().confirm((CallRequest.MerchantConfirm) request, key));
        } else {
            OutboundCall call = new OutboundCall("sandbox." + (++calls), m, target, key, request);
            world.gateway().handle(call, facts::putAll);
        }
        if (facts.isEmpty()) {
            respond(exchange, 502, json("error", "the world gave no answer"));   // never with faults off
            return;
        }
        reply(exchange, CallWire.encodeReply(request, facts));
    }

    private static void reply(HttpExchange exchange, CallWire.Reply r) throws IOException {
        respond(exchange, r.status(), new String(r.body(), StandardCharsets.UTF_8));
    }

    private static String json(String key, String value) {
        return new String(StrictJson.write(g -> g.writeStringProperty(key, value)), StandardCharsets.UTF_8) + "\n";
    }

    private static Map<String, String> query(URI uri) {
        TreeMap<String, String> out = new TreeMap<>();
        String raw = uri.getRawQuery();
        if (raw == null) {
            return out;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                out.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", body.startsWith("{") ? "application/json" : "text/plain");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
