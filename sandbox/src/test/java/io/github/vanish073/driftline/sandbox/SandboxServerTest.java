package io.github.vanish073.driftline.sandbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.wire.call.CallWire;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The sandbox's gateway, merchant and ops APIs (D65), called over HTTP through the same {@link CallWire} as Driftline's
 * dispatcher: an injected world answers about its payments, refunds once per key, tells Driftline about the refund by
 * webhook, and counts what it did.
 */
class SandboxServerTest {

    static final HttpClient HTTP = HttpClient.newHttpClient();

    HttpServer driftline;
    SandboxServer sandbox;
    String base;
    final AtomicInteger refundWebhooks = new AtomicInteger();

    @BeforeEach
    void start() throws IOException {
        driftline = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        driftline.createContext("/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (exchange.getRequestURI().getPath().startsWith("/v1/webhooks/gateway/") && body.contains("refund.processed")) {
                refundWebhooks.incrementAndGet();
            }
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        });
        driftline.start();
        BootstrapFile.Pool pool = BootstrapFile.read(BootstrapFile.generate(2, 12,
                IstCalendar.istDate(Instant.now()).minusDays(3), new SecureRandom()));
        sandbox = new SandboxServer(0, URI.create("http://127.0.0.1:" + driftline.getAddress().getPort()), pool);
        sandbox.start();
        base = "http://127.0.0.1:" + sandbox.port();
    }

    @AfterEach
    void stop() {
        sandbox.stop();
        driftline.stop(0);
    }

    private HttpResponse<byte[]> send(String method, String path, byte[] body, Optional<String> key) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                .method(method, body.length == 0 ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofByteArray(body));
        key.ifPresent(k -> b.header("Idempotency-Key", k));
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    /** One call as the dispatcher makes it; returns the facts it decodes, or empty for no result. */
    private Optional<TreeMap<String, String>> call(String prefix, OutboundCall call) throws Exception {
        CallWire.HttpCall http = CallWire.request(call);
        HttpResponse<byte[]> r = send(http.method(), prefix + http.path(), http.body(), call.idempotencyKey());
        return CallWire.reply(call.request(), r.statusCode(), r.body());
    }

    private JsonObject stats() throws Exception {
        return StrictJson.parse(send("GET", "/v1/stats", new byte[0], Optional.empty()).body());
    }

    @Test
    void anInjectedWorldAnswersTheDispatchersCallsAndRefundsOncePerKey() throws Exception {
        JsonObject injected = StrictJson.parse(send("POST", "/v1/inject?type=DUPLICATE_CAPTURE&count=1", new byte[0],
                Optional.empty()).body());
        MerchantId m = MerchantId.of(injected.string("merchant_id"));
        String expected = injected.string("expected_breaks");   // "DUPLICATE_CAPTURE m_e2e_01/pay_0_b"
        PaymentId duplicate = PaymentId.of(expected.substring(expected.indexOf('/') + 1));

        TreeMap<String, String> status = call("/gateway", new OutboundCall("c.1", m, CallTarget.GATEWAY,
                Optional.empty(), new CallRequest.StatusQuery(duplicate))).orElseThrow();
        assertThat(status).containsEntry("status", "SUCCEEDED");
        OrderId order = OrderId.of(status.get("order_id"));
        Money amount = Money.parse(status.get("amount"));
        assertThat(call("/gateway", new OutboundCall("c.2", m, CallTarget.GATEWAY, Optional.empty(),
                new CallRequest.StatusQueryOrder(order))).orElseThrow()).containsEntry("count", "2");
        assertThat(call("/gateway", new OutboundCall("c.3", m, CallTarget.GATEWAY, Optional.empty(),
                new CallRequest.StatusQuery(PaymentId.of("pay_nobody"))))).contains(new TreeMap<>(Map.of("status", "UNKNOWN")));

        CallRequest.Refund refund = new CallRequest.Refund(duplicate, order, amount);
        TreeMap<String, String> first = call("/gateway", new OutboundCall("c.4", m, CallTarget.GATEWAY,
                Optional.of("rfd_key_1"), refund)).orElseThrow();
        TreeMap<String, String> again = call("/gateway", new OutboundCall("c.5", m, CallTarget.GATEWAY,
                Optional.of("rfd_key_1"), refund)).orElseThrow();
        assertThat(first).containsEntry("status", "PROCESSED").containsKey("refund_id");
        assertThat(again).as("a retry with the same key gets the first answer").isEqualTo(first);
        assertThat(stats().integer("refunds")).isEqualTo(1);
        assertThat(stats().integer("payments_refunded_more_than_once")).isZero();
        assertThat(call("/gateway", new OutboundCall("c.6", m, CallTarget.GATEWAY, Optional.empty(),
                new CallRequest.RefundLookup(duplicate))).orElseThrow())
                .as("the lookup API is eventually consistent (20 s)").containsEntry("found", "no");

        long deadline = System.currentTimeMillis() + 5_000;
        while (refundWebhooks.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(refundWebhooks).as("the ticker delivers the gateway's refund webhook").hasValue(1);

        assertThat(call("/gateway", new OutboundCall("c.7", m, CallTarget.GATEWAY, Optional.of("rfd_other_key"),
                refund)).orElseThrow()).containsEntry("status", "PROCESSED");
        assertThat(stats().integer("payments_refunded_more_than_once"))
                .as("the gateway dedupes by key only, so a new key refunds again (DESIGN §2.2)").isEqualTo(1);
    }

    @Test
    void theMerchantAnswersConfirmationsAndTheOpsSinkCountsEscalationsOnce() throws Exception {
        JsonObject injected = StrictJson.parse(send("POST", "/v1/inject?type=DUPLICATE_CAPTURE&count=1", new byte[0],
                Optional.empty()).body());
        MerchantId m = MerchantId.of(injected.string("merchant_id"));
        String expected = injected.string("expected_breaks");
        PaymentId duplicate = PaymentId.of(expected.substring(expected.indexOf('/') + 1));
        TreeMap<String, String> status = call("/gateway", new OutboundCall("c.1", m, CallTarget.GATEWAY,
                Optional.empty(), new CallRequest.StatusQuery(duplicate))).orElseThrow();
        assertThat(call("/merchant", new OutboundCall("c.2", m, CallTarget.MERCHANT, Optional.of("cfm_1"),
                new CallRequest.MerchantConfirm(OrderId.of(status.get("order_id")), duplicate,
                        Money.parse(status.get("amount")))))).as("the order is paid with the other payment")
                .contains(new TreeMap<>(Map.of("status", "REJECTED")));

        OutboundCall notice = new OutboundCall("c.3", m, CallTarget.OPS, Optional.empty(),
                new CallRequest.EscalationNotify("brk_1", BreakType.SHORT_SETTLEMENT, "m/stl:1", "short"));
        assertThat(call("/ops", notice)).contains(new TreeMap<>(Map.of("status", "DELIVERED")));
        assertThat(call("/ops", notice)).contains(new TreeMap<>(Map.of("status", "DELIVERED")));
        assertThat(stats().integer("escalations")).isEqualTo(1);
    }

    @Test
    void anUnknownRouteIs404AndABadBody400() throws Exception {
        assertThat(send("DELETE", "/gateway/v1/merchants/m_e2e_01/payments/pay_1", new byte[0], Optional.empty())
                .statusCode()).isEqualTo(404);
        assertThat(send("POST", "/gateway/v1/merchants/m_e2e_01/payments/pay_1/refunds",
                "{\"order_id\":\"ord_1\"}".getBytes(StandardCharsets.UTF_8), Optional.empty()).statusCode())
                .isEqualTo(400);
    }
}
