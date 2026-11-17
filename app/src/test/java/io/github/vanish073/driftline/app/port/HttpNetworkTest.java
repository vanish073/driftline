package io.github.vanish073.driftline.app.port;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The production Network: non-blocking send, outcome in the inbox, the idempotency key on the wire (§3.2). */
class HttpNetworkTest {


    private HttpServer server;
    private final AtomicReference<String> seenKey = new AtomicReference<>();
    private final BlockingQueue<HttpNetwork.CallOutcome> inbox = new ArrayBlockingQueue<>(4);

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ping", exchange -> {
            seenKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            byte[] body = ("pong:" + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private HttpNetwork network(Map<Class<? extends CallRequest>, HttpNetwork.CallCodec> codecs) {
        URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        return new HttpNetwork(HttpClient.newHttpClient(), Map.of(CallTarget.GATEWAY, base), codecs, inbox::add,
                Duration.ofSeconds(5));
    }

    private static OutboundCall call(CallRequest request) {
        return new OutboundCall("call-1", MerchantId.of("m_1"), CallTarget.GATEWAY, Optional.of("refund-key-1"), request);
    }

    @Test
    void theOutcomeArrivesInTheInboxWithTheIdempotencyKeySent() throws InterruptedException {
        HttpNetwork network = network(Map.of(CallRequest.StatusQuery.class, (c, base) -> HttpRequest.newBuilder(
                base.resolve("/ping")).POST(HttpRequest.BodyPublishers.ofString(
                        ((CallRequest.StatusQuery) c.request()).paymentId().value()))));
        network.send(call(new CallRequest.StatusQuery(PaymentId.of("hello"))));
        HttpNetwork.CallOutcome outcome = inbox.poll(10, TimeUnit.SECONDS);
        assertThat(outcome).isNotNull();
        assertThat(outcome.status()).contains(201);
        assertThat(new String(outcome.body(), StandardCharsets.UTF_8)).isEqualTo("pong:hello");
        assertThat(outcome.error()).isEmpty();
        assertThat(seenKey.get()).isEqualTo("refund-key-1");
    }

    @Test
    void aCallWithNoCodecComesBackAsAnErrorAtOnceAndIsNeverDropped() throws InterruptedException {
        network(Map.of()).send(call(new CallRequest.StatusQuery(PaymentId.of("x"))));
        HttpNetwork.CallOutcome outcome = inbox.poll(1, TimeUnit.SECONDS);
        assertThat(outcome).isNotNull();
        assertThat(outcome.status()).isEmpty();
        assertThat(outcome.error()).contains("no codec for StatusQuery");
    }

    @Test
    void aTransportFailureIsAnOutcomeToo() throws InterruptedException {
        HttpNetwork network = network(Map.of(CallRequest.StatusQuery.class,
                (c, base) -> HttpRequest.newBuilder(base.resolve("/ping"))));
        server.stop(0);
        network.send(call(new CallRequest.StatusQuery(PaymentId.of("x"))));
        HttpNetwork.CallOutcome outcome = inbox.poll(10, TimeUnit.SECONDS);
        assertThat(outcome).isNotNull();
        assertThat(outcome.status()).isEmpty();
        assertThat(outcome.error()).isPresent();
    }
}
