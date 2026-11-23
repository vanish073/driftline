package io.github.vanish073.driftline.bench;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * The open-model load generator (DESIGN §17): events arrive at a constant rate whatever the responses do, so a slow
 * system cannot slow the arrivals down and hide its own latency (coordinated omission). Each order is three events
 * through the real ingest API, in arrival order: the merchant's {@code order.created} (bearer token, Idempotency-Key),
 * the gateway's signed {@code payment.succeeded} webhook (with a fake customer contact, D77), and the merchant's
 * {@code order.paid}; orders go to the bootstrap file's merchants in turn, with event times of now, so the matcher
 * works in real time. At most {@code --max-in-flight} requests are outstanding; an arrival beyond that is not sent and
 * counts as skipped, which makes the step fail rather than quietly lowering the rate.
 *
 * <pre>
 *   --url http://127.0.0.1:8080  --bootstrap FILE  --rate EVENTS_PER_SECOND  --seconds N  --label s100
 *   [--max-in-flight 1024]  --out step.json
 * </pre>
 */
public final class LoadGenerator {

    /** One merchant of the bootstrap file. */
    record Merchant(String id, byte[] secret, String token) {
    }

    /** A request to send at an arrival slot. */
    record Arrival(String merchant, URI uri, byte[] body, Map<String, String> headers) {
    }

    static List<Merchant> merchants(byte[] bootstrap) {
        JsonObject root = StrictJson.parse(bootstrap);
        JsonObject all = root.object("merchants");
        List<Merchant> out = new ArrayList<>();
        for (String id : new TreeMap<>(all.fields()).keySet()) {
            JsonObject m = all.object(id);
            out.add(new Merchant(id, m.string("webhook_secret").getBytes(StandardCharsets.US_ASCII), m.string("token")));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("the bootstrap file has no merchants");
        }
        return List.copyOf(out);
    }

    /**
     * The request for arrival slot {@code slot}: order {@code slot / 3}, its step {@code slot % 3}. Ids carry the
     * label, so steps never reuse a key.
     */
    static Arrival arrival(URI base, List<Merchant> merchants, String label, long slot, Instant clock) {
        Instant now = clock.truncatedTo(ChronoUnit.MILLIS);   // the codecs refuse sub-millisecond times
        long n = slot / 3;
        int step = (int) (slot % 3);
        Merchant m = merchants.get((int) (n % merchants.size()));
        OrderId order = OrderId.of("ord_" + label + "_" + n);
        PaymentId payment = PaymentId.of("pay_" + label + "_" + n);
        Money amount = Money.ofPaise(49_900);
        Instant expires = now.plus(15, ChronoUnit.MINUTES);
        if (step == 1) {
            // a fake customer contact, as a real gateway's webhook carries one (D77); the same pattern the sandbox
            // uses, so scripts/pii-scan.sh checks that none of it leaks
            String k = String.format(Locale.ROOT, "%06d", n % 1_000_000);
            CustomerContact contact = CustomerContact.of(Optional.of("buyer" + k + "@okaxis"), Optional.of("6000" + k),
                    Optional.of("buyer" + k + "@example.com"));
            byte[] body = GatewayWebhookCodec.encode(new GatewayWebhook("evt_" + label + "_" + n,
                    EventType.PAYMENT_SUCCEEDED, now, new PaymentPayload(payment, order, amount)), Optional.of(contact));
            return new Arrival(m.id(), base.resolve("/v1/webhooks/gateway/" + m.id()), body,
                    Map.of(WebhookSignature.HEADER, WebhookSignature.sign(m.secret(), now, body)));
        }
        boolean paid = step == 2;
        byte[] body = OrderEventCodec.encode(new OrderEventMessage(paid ? EventType.ORDER_PAID : EventType.ORDER_CREATED,
                now, new OrderPayload(order, paid ? 2 : 1, amount, expires, paid ? Optional.of(payment)
                        : Optional.empty())));
        return new Arrival(m.id(), base.resolve("/v1/merchants/" + m.id() + "/order-events"), body,
                Map.of("Authorization", "Bearer " + m.token(), "Idempotency-Key", label + "-" + n + "-" + step));
    }

    /** Thread-safe latency samples, in nanoseconds. */
    static final class Samples {
        private long[] values = new long[4_096];
        private int size;

        synchronized void add(long nanos) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
            }
            values[size++] = nanos;
        }

        synchronized long[] sorted() {
            long[] out = Arrays.copyOf(values, size);
            Arrays.sort(out);
            return out;
        }
    }

    public static void main(String[] args) throws Exception {
        URI base = URI.create("http://127.0.0.1:8080");
        Path bootstrap = null;
        double rate = 0;
        int seconds = 0;
        String label = null;
        int maxInFlight = 1_024;
        Path out = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url" -> base = URI.create(args[++i]);
                case "--bootstrap" -> bootstrap = Path.of(args[++i]);
                case "--rate" -> rate = Double.parseDouble(args[++i]);
                case "--seconds" -> seconds = Integer.parseInt(args[++i]);
                case "--label" -> label = args[++i];
                case "--max-in-flight" -> maxInFlight = Integer.parseInt(args[++i]);
                case "--out" -> out = Path.of(args[++i]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        if (bootstrap == null || rate <= 0 || seconds <= 0 || label == null || out == null
                || !label.matches("[a-z0-9]{1,12}")) {
            throw new IllegalArgumentException("--bootstrap, --rate, --seconds, --label ([a-z0-9]{1,12}) and --out");
        }
        List<Merchant> merchants = merchants(Files.readAllBytes(bootstrap));
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();
        AtomicLong sent = new AtomicLong();
        AtomicLong skipped = new AtomicLong();
        AtomicLong transportErrors = new AtomicLong();
        TreeMap<Integer, AtomicLong> statuses = new TreeMap<>();
        Samples latency = new Samples();
        List<String> firstRejections = new ArrayList<>();
        long total = (long) Math.floor(rate * seconds);
        double intervalNanos = 1e9 / rate;
        long start = System.nanoTime();
        for (long slot = 0; slot < total; slot++) {
            long due = start + (long) (slot * intervalNanos);
            long wait = due - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
            }
            if (inFlight.get() >= maxInFlight) {
                skipped.incrementAndGet();
                continue;
            }
            Arrival a = arrival(base, merchants, label, slot, Instant.now());
            HttpRequest.Builder request = HttpRequest.newBuilder(a.uri()).timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(a.body()));
            a.headers().forEach(request::header);
            int now = inFlight.incrementAndGet();
            maxSeen.accumulateAndGet(now, Math::max);
            sent.incrementAndGet();
            long t0 = System.nanoTime();
            client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString()).whenComplete((response, error) -> {
                latency.add(System.nanoTime() - t0);
                if (error != null) {
                    transportErrors.incrementAndGet();
                } else {
                    synchronized (statuses) {
                        statuses.computeIfAbsent(response.statusCode(), k -> new AtomicLong()).incrementAndGet();
                        if (response.statusCode() != 202 && firstRejections.size() < 3) {
                            String body = response.body().replace('\n', ' ').replace('\r', ' ');
                            firstRejections.add(response.statusCode() + " " + a.uri().getPath() + " "
                                    + (body.length() > 200 ? body.substring(0, 200) : body));
                        }
                    }
                }
                inFlight.decrementAndGet();
            });
        }
        long scheduleEnd = System.nanoTime();
        long drainUntil = scheduleEnd + 30_000_000_000L;
        while (inFlight.get() > 0 && System.nanoTime() < drainUntil) {
            LockSupport.parkNanos(10_000_000L);
        }
        long[] lat = latency.sorted();
        long accepted;
        long rejected;
        String statusJson;
        String rejectionsJson;
        synchronized (statuses) {
            rejectionsJson = firstRejections.stream()
                    .map(r -> '"' + r.replace("\\", "\\\\").replace("\"", "\\\"") + '"')
                    .collect(Collectors.joining(",", "[", "]"));
            accepted = statuses.getOrDefault(202, new AtomicLong()).get();
            rejected = statuses.entrySet().stream().filter(e -> e.getKey() != 202).mapToLong(e -> e.getValue().get())
                    .sum();
            StringBuilder sj = new StringBuilder("{");
            statuses.forEach((k, v) -> sj.append(sj.length() > 1 ? "," : "").append('"').append(k).append("\":")
                    .append(v.get()));
            statusJson = sj.append('}').toString();
        }
        double actualSeconds = (scheduleEnd - start) / 1e9;
        // the generator's own CPU, so a busy machine can be attributed (it shares the laptop with the stack)
        double cpuSeconds = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime() / 1e9;
        String json = String.format(Locale.ROOT, """
                {"label":"%s","offered_rate":%.1f,"seconds":%d,"schedule_seconds":%.1f,"arrivals":%d,"sent":%d,\
                "accepted":%d,"rejected":%d,"transport_errors":%d,"skipped":%d,"still_in_flight":%d,\
                "max_in_flight":%d,"statuses":%s,"first_rejections":%s,"http_p50_ms":%.2f,"http_p99_ms":%.2f,\
                "http_max_ms":%.2f,"cpu_seconds":%.1f}
                """, label, rate, seconds, actualSeconds, total, sent.get(), accepted, rejected, transportErrors.get(),
                skipped.get(), inFlight.get(), maxSeen.get(), statusJson, rejectionsJson, HotAccountBenchmark.millis(lat, 0.50),
                HotAccountBenchmark.millis(lat, 0.99), lat.length == 0 ? Double.NaN : lat[lat.length - 1] / 1e6,
                cpuSeconds);
        Files.writeString(out, json, StandardCharsets.UTF_8);
        System.out.print("load: " + json);
        System.exit(0);
    }

    private LoadGenerator() {
    }
}
