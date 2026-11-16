package io.github.vanish073.driftline.sandbox;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sim.world.Scenario;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The sandbox over the JDK HttpServer, no Spring (D20, D51). Its control API injects scenarios into a running
 * Driftline: {@code POST /v1/inject?type=DUPLICATE_CAPTURE&count=5} takes the next unused merchant of the bootstrap
 * pool, plays the scenario with the simulator's world models, and answers with what Driftline must now show.
 * Requests are handled one at a time. {@code GET /health} answers {@code ok}. The gateway's status and refund APIs
 * join this server with the dispatcher in Shift 8.
 */
final class SandboxServer {

    private final HttpServer server;
    private final URI driftline;
    private final BootstrapFile.Pool pool;
    private final Deque<BootstrapFile.Merchant> unused;

    SandboxServer(int port, URI driftline, BootstrapFile.Pool pool) throws IOException {
        this.driftline = driftline;
        this.pool = pool;
        this.unused = new ArrayDeque<>(pool.merchants());
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> respond(exchange, 200, "ok\n"));
        server.createContext("/v1/inject", this::inject);
    }

    void start() {
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    private synchronized void inject(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, json("error", "POST only"));
            return;
        }
        Map<String, String> query = query(exchange.getRequestURI());
        Scenario scenario;
        int count;
        try {
            scenario = Scenario.valueOf(query.getOrDefault("type", "").toUpperCase(Locale.ROOT));
            count = Integer.parseInt(query.getOrDefault("count", "1"));
        } catch (IllegalArgumentException e) {
            respond(exchange, 400, json("error", "type must be a scenario and count a number"));
            return;
        }
        if (!Injection.oneToOne().contains(scenario) && scenario != Scenario.NORMAL) {
            respond(exchange, 400, json("error", "type must be NORMAL or a one-to-one break scenario"));
            return;
        }
        BootstrapFile.Merchant merchant = unused.pollFirst();
        if (merchant == null) {
            respond(exchange, 409, json("error", "every merchant in the pool has been used; restart with a new pool"));
            return;
        }
        try {
            Injection.Report r = Injection.run(merchant, scenario, count, new HttpDriftline(driftline, pool.merchants()),
                    IstCalendar.istDate(Instant.now()));
            respond(exchange, 200, new String(StrictJson.write(g -> {
                g.writeStringProperty("merchant_id", r.merchantId().value());
                g.writeNumberProperty("partition", merchant.partition());
                g.writeStringProperty("type", r.scenario().name());
                g.writeNumberProperty("count", r.count());
                g.writeStringProperty("last_file_date", r.lastFileDate().toString());
                g.writeNumberProperty("expected_count", r.expected().size());
                g.writeStringProperty("expected_breaks", String.join(";", r.expected()));
                g.writeNumberProperty("webhooks", r.webhooks());
                g.writeNumberProperty("order_events", r.orderEvents());
                g.writeNumberProperty("settlement_files", r.files());
                g.writeNumberProperty("not_accepted", r.notAccepted());
            }), StandardCharsets.UTF_8) + "\n");
        } catch (RuntimeException e) {
            respond(exchange, 500, json("error", e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
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
