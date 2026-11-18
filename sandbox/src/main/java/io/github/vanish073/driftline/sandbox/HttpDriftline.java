package io.github.vanish073.driftline.sandbox;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.sim.world.Ingress;
import io.github.vanish073.driftline.sim.world.Request;
import io.github.vanish073.driftline.sim.world.SettlementUploads;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

/**
 * The world models' view of a real Driftline over HTTP: the simulator's {@link Ingress} and {@link SettlementUploads}
 * as real requests to the ingest API (D51). The world runs on virtual time in the past, so its webhooks are re-signed
 * here with the real clock: each delivery attempt is signed with a fresh timestamp anyway (§10.2), and the event time
 * inside the body stays the world's. A transport error or a 503 is retried a few times in real time before the world
 * sees a failure (and retries on its own schedule). A payment webhook gains a fake customer contact here
 * ({@link FakeContacts}, D77), as a real gateway's would carry; the simulator itself never adds one.
 */
final class HttpDriftline implements Ingress, SettlementUploads {

    private static final int ATTEMPTS = 20;
    private static final Duration BACKOFF = Duration.ofMillis(500);

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final URI base;
    private final TreeMap<MerchantId, BootstrapFile.Merchant> merchants = new TreeMap<>();
    final AtomicLong webhooks = new AtomicLong();
    final AtomicLong orderEvents = new AtomicLong();
    final AtomicLong files = new AtomicLong();
    final AtomicLong notAccepted = new AtomicLong();

    HttpDriftline(URI base, Iterable<BootstrapFile.Merchant> pool) {
        this.base = base;
        for (BootstrapFile.Merchant m : pool) {
            merchants.put(m.id(), m);
        }
    }

    @Override
    public void receive(Request request, IntConsumer reply) {
        BootstrapFile.Merchant m = merchants.get(request.merchantId());
        switch (request) {
            case Request.Webhook w -> {
                webhooks.incrementAndGet();
                byte[] body = FakeContacts.addTo(w.body());
                reply.accept(post("/v1/webhooks/gateway/" + m.id().value(), body, () -> Map.of(
                        WebhookSignature.HEADER, WebhookSignature.sign(m.secret().getBytes(StandardCharsets.US_ASCII),
                                Instant.now(), body))));
            }
            case Request.OrderEvent o -> {
                orderEvents.incrementAndGet();
                reply.accept(post("/v1/merchants/" + m.id().value() + "/order-events", o.body(), () -> Map.of(
                        "Authorization", "Bearer " + m.token(), "Idempotency-Key", o.idempotencyKey().orElseThrow())));
            }
            case Request.Observation o ->
                    throw new IllegalArgumentException("observations come from Driftline's dispatcher, never the world");
        }
    }

    @Override
    public void upload(MerchantId merchantId, byte[] file, IntConsumer reply) {
        BootstrapFile.Merchant m = merchants.get(merchantId);
        files.incrementAndGet();
        reply.accept(post("/v1/merchants/" + m.id().value() + "/settlement-files", file,
                () -> Map.of("Authorization", "Bearer " + m.token())));
    }

    /** Headers are built per attempt, so every webhook attempt carries a fresh signature timestamp. */
    private int post(String path, byte[] body, java.util.function.Supplier<Map<String, String>> headers) {
        int status = 599;
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            HttpRequest.Builder request = HttpRequest.newBuilder(base.resolve(path)).timeout(Duration.ofSeconds(20))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            headers.get().forEach(request::header);
            try {
                status = client.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                if (status != 503) {
                    break;
                }
            } catch (IOException e) {
                status = 599;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 599;
            }
            sleep();
        }
        if (status < 200 || status > 299) {
            notAccepted.incrementAndGet();
        }
        return status;
    }

    private static void sleep() {
        try {
            Thread.sleep(BACKOFF);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
