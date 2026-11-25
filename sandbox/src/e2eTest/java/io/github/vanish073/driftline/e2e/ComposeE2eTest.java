package io.github.vanish073.driftline.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.sandbox.BootstrapFile;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import io.github.vanish073.driftline.wire.order.OrderEventCodec;
import io.github.vanish073.driftline.wire.order.OrderEventMessage;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhook;
import io.github.vanish073.driftline.wire.webhook.GatewayWebhookCodec;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Milestone M3 (REQUIREMENTS: "one-to-one breaks are detected end to end in Docker Compose"; DESIGN §15.2, D51).
 * Each one-to-one break scenario is injected through the sandbox, which plays it with the simulator's world models
 * against the running stack; every break the simulator's Oracle expects must then be visible through the ops API
 * within the time limit, and no other one-to-one break may appear for that merchant except one that already carries
 * the evidence that heals it (D52: the injected world lies behind the watermarks, so a check can fire before the
 * merchant's event of the next poll chunk arrives). Milestone M4 (Shift 8): every one of those breaks is then handled
 * by the resolver and the dispatcher against the sandbox's gateway, merchant and ops APIs (refunded, posted as fees,
 * held in suspense or escalated), no payment is refunded twice, an escalated break is resolved by hand through the
 * audited API, and a batch line that two sets of payments fit is reported as AMBIGUOUS with both witnesses and
 * escalated (D68). Then the ingest security checks: a replayed webhook is harmless, a bad signature is 401, a stale
 * timestamp is rejected, a cross-merchant token is 403.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ComposeE2eTest {

    static final Duration DETECTION_LIMIT = Duration.ofSeconds(Long.parseLong(
            Optional.ofNullable(System.getenv("DETECTION_LIMIT_SECONDS")).orElse("300")));
    static final List<String> ONE_TO_ONE = List.of("DEBITED_NO_ORDER", "LATE_SUCCESS", "DUPLICATE_CAPTURE",
            "SHORT_SETTLEMENT", "MISSING_SETTLEMENT", "ORPHAN_SETTLEMENT");
    static final int PER_TYPE = 3;
    /**
     * D52: the one evidence key that heals each break type when it is raised before the late evidence arrives (the
     * resolver makes it SELF_HEALED in Shift 8; the simulator counts the same keys as {@code breaks.late_evidence}).
     */
    static final Map<String, String> HEALED_BY = Map.ofEntries(
            Map.entry("DEBITED_NO_ORDER", "order_paid_later"),
            Map.entry("DUPLICATE_CAPTURE", "kept_by_order"),
            Map.entry("ORPHAN_SETTLEMENT", "attributed_to"));

    /** The pool's last two merchants are kept for the security checks; injections take merchants from the front. */
    static final BootstrapFile.Merchant A = Api.POOL.merchants().get(Api.POOL.merchants().size() - 1);
    static final BootstrapFile.Merchant B = Api.POOL.merchants().get(Api.POOL.merchants().size() - 2);

    record Injected(String type, String merchant, TreeSet<String> expected, Instant at) {
    }

    /** What test 1 injected; the handling tests (M4) follow those breaks. */
    static final List<Injected> INJECTED = new ArrayList<>();

    @Test
    @Order(1)
    void everyOneToOneBreakIsDetectedEndToEndAndVisibleThroughTheOpsApi() throws InterruptedException {
        List<Injected> injected = INJECTED;
        for (String type : ONE_TO_ONE) {
            HttpResponse<String> r = Api.send(HttpRequest.newBuilder(Api.SANDBOX.resolve("/v1/inject?type=" + type
                    + "&count=" + PER_TYPE)).POST(HttpRequest.BodyPublishers.noBody()));
            assertThat(r.statusCode()).as("inject %s: %s", type, r.body()).isEqualTo(200);
            JsonObject report = StrictJson.parse(r.body().getBytes(StandardCharsets.UTF_8));
            assertThat(report.integer("not_accepted")).as("every injected request was accepted (%s)", type).isZero();
            TreeSet<String> expected = new TreeSet<>(Arrays.asList(report.string("expected_breaks").split(";")));
            expected.remove("");
            assertThat(expected).as("the world of %s implies at least one %s", type, type)
                    .anyMatch(k -> k.startsWith(type + " "));
            injected.add(new Injected(type, report.string("merchant_id"), expected, Instant.now()));
            System.out.printf("injected %-18s merchant=%s partition=%d expected=%d webhooks=%d order_events=%d files=%d%n",
                    type, report.string("merchant_id"), report.integer("partition"), expected.size(),
                    report.integer("webhooks"), report.integer("order_events"), report.integer("settlement_files"));
        }

        Map<String, Duration> detectedAfter = new LinkedHashMap<>();
        Instant deadline = Instant.now().plus(DETECTION_LIMIT);
        TreeMap<String, TreeMap<String, String>> raised = new TreeMap<>();
        while (detectedAfter.size() < injected.size() && Instant.now().isBefore(deadline)) {
            raised = raisedOneToOne();
            for (Injected i : injected) {
                if (!detectedAfter.containsKey(i.type())
                        && raised.getOrDefault(i.merchant(), new TreeMap<>()).keySet().containsAll(i.expected())) {
                    detectedAfter.put(i.type(), Duration.between(i.at(), Instant.now()));
                }
            }
            Thread.sleep(2_000);
        }
        raised = raisedOneToOne();
        System.out.println("detection (from the end of each injection until every expected break was visible):");
        for (Injected i : injected) {
            System.out.printf("  %-18s %s%n", i.type(), Optional.ofNullable(detectedAfter.get(i.type()))
                    .map(d -> d.toSeconds() + " s").orElse("NOT DETECTED within " + DETECTION_LIMIT.toSeconds() + " s"));
        }
        // A break raised at the end of one poll chunk gets its healing evidence in the next one: let that land.
        Instant settle = Instant.now().plusSeconds(15);
        while (unhealedExtras(raised, injected) > 0 && Instant.now().isBefore(settle)) {
            Thread.sleep(1_000);
            raised = raisedOneToOne();
        }
        int healed = 0;
        for (Injected i : injected) {
            TreeMap<String, String> mine = raised.getOrDefault(i.merchant(), new TreeMap<>());
            assertThat(mine.keySet())
                    .as("%s: every one-to-one break the world implies for %s is raised", i.type(), i.merchant())
                    .containsAll(i.expected());
            TreeMap<String, String> extra = new TreeMap<>(mine);
            extra.keySet().removeAll(i.expected());
            for (Map.Entry<String, String> e : extra.entrySet()) {
                String type = e.getKey().substring(0, e.getKey().indexOf(' '));
                assertThat(Arrays.asList(e.getValue().split(",")))
                        .as("%s: %s is not implied by the world, so it must carry its healing evidence %s (D52);"
                                + " evidence keys: [%s]", i.type(), e.getKey(), HEALED_BY.get(type), e.getValue())
                        .contains(Optional.ofNullable(HEALED_BY.get(type)).orElse("(none heals " + type + ")"));
                System.out.printf("  raised before its late evidence, healed by %s: %s%n", HEALED_BY.get(type),
                        e.getKey());
                healed++;
            }
        }
        System.out.printf("one-to-one breaks: %d expected, all raised; %d raised early and already healed (D52)%n",
                injected.stream().mapToInt(i -> i.expected().size()).sum(), healed);
        assertThat(detectedAfter).as("every type detected within the limit").hasSize(injected.size());
    }

    // ---- M4: every break handled (Shift 8) -------------------------------------------------------------------------

    /**
     * The states that count as handled for each type, in a world that lies days in the past (D51): a refund closes
     * DEBITED_NO_ORDER and LATE_SUCCESS (whose order expired long ago, so the merchant rejects the late success and
     * the customer is refunded); DUPLICATE_CAPTURE is past its T+1 escalate_by, so it is escalated with its
     * compensation exposure; an itemised SHORT_SETTLEMENT is posted as fees, any other is escalated; MISSING_SETTLEMENT
     * is escalated; an ORPHAN_SETTLEMENT's money waits in suspense for attribution until escalate_by (4 h). A late
     * success the merchant accepts and fulfils needs an order that has not expired, so the simulator proves that path.
     */
    static final Map<String, List<String>> HANDLED_AS = Map.of(
            "DEBITED_NO_ORDER", List.of("RESOLVED_AUTO", "SELF_HEALED", "LINKED"),
            "LATE_SUCCESS", List.of("RESOLVED_AUTO", "SELF_HEALED", "LINKED"),
            "DUPLICATE_CAPTURE", List.of("ESCALATED", "RESOLVED_AUTO", "SELF_HEALED", "LINKED"),
            "SHORT_SETTLEMENT", List.of("RESOLVED_AUTO", "ESCALATED"),
            "MISSING_SETTLEMENT", List.of("ESCALATED"),
            "ORPHAN_SETTLEMENT", List.of("AWAITING_EVIDENCE", "SELF_HEALED", "ESCALATED"));
    static final List<String> REFUND_TYPES = List.of("DEBITED_NO_ORDER", "LATE_SUCCESS", "DUPLICATE_CAPTURE");

    /** {@code "TYPE merchant/subject" → break} for every one-to-one break, with its id and state. */
    static TreeMap<String, Map<String, String>> oneToOneBreaks() {
        HttpResponse<String> r = Api.get("/v1/breaks", Api.POOL.opsToken());
        assertThat(r.statusCode()).isEqualTo(200);
        TreeMap<String, Map<String, String>> out = new TreeMap<>();
        for (Map<String, String> b : Api.breaks(r.body())) {
            if (ONE_TO_ONE.contains(b.get("type"))) {
                out.put(b.get("type") + " " + b.get("merchant_id") + "/" + b.get("subject_key"), b);
            }
        }
        return out;
    }

    static JsonObject sandboxStats() {
        HttpResponse<String> r = Api.send(HttpRequest.newBuilder(Api.SANDBOX.resolve("/v1/stats")).GET());
        assertThat(r.statusCode()).isEqualTo(200);
        return StrictJson.parse(r.body().getBytes(StandardCharsets.UTF_8));
    }

    /** The reason of a break's last transition, from {@code GET /v1/breaks/{id}}. */
    static String lastReason(String breakId) {
        String body = Api.get("/v1/breaks/" + breakId, Api.POOL.opsToken()).body();
        int at = body.lastIndexOf("\"reason\":\"");
        return at < 0 ? "" : body.substring(at + 10, body.indexOf('"', at + 10));
    }

    @Test
    @Order(2)
    void everyInjectedBreakIsHandledAndNoPaymentIsRefundedTwice() throws InterruptedException {
        assertThat(INJECTED).as("test 1 injected every type").hasSize(ONE_TO_ONE.size());
        TreeSet<String> expected = new TreeSet<>();
        INJECTED.forEach(i -> expected.addAll(i.expected()));
        Instant deadline = Instant.now().plus(DETECTION_LIMIT);
        TreeMap<String, Map<String, String>> breaks = oneToOneBreaks();
        while (!allHandled(expected, breaks) && Instant.now().isBefore(deadline)) {
            Thread.sleep(2_000);
            breaks = oneToOneBreaks();
        }
        TreeMap<String, TreeMap<String, Integer>> byType = new TreeMap<>();
        long refunded = 0;
        long escalated = 0;
        for (String key : expected) {
            String type = key.substring(0, key.indexOf(' '));
            Map<String, String> b = breaks.get(key);
            assertThat(b).as("%s is visible", key).isNotNull();
            assertThat(HANDLED_AS.get(type)).as("%s is handled (state %s)", key, b.get("state")).contains(b.get("state"));
            byType.computeIfAbsent(type, t -> new TreeMap<>()).merge(b.get("state"), 1, Integer::sum);
            if (REFUND_TYPES.contains(type) && b.get("state").equals("RESOLVED_AUTO")
                    && lastReason(b.get("break_id")).equals("refund confirmed")) {
                refunded++;
            }
            if (b.get("state").equals("ESCALATED")) {
                escalated++;
            }
        }
        System.out.println("handled (state counts per type, for every break the world implies):");
        byType.forEach((type, states) -> System.out.printf("  %-18s %s%n", type, states));
        assertThat(byType.keySet()).as("all six types handled").containsExactlyInAnyOrderElementsOf(ONE_TO_ONE);
        assertThat(refunded).as("breaks closed by a confirmed refund").isPositive();

        Instant notices = Instant.now().plusSeconds(60);
        JsonObject stats = sandboxStats();
        while ((stats.integer("refunds") < refunded || stats.integer("escalations") < escalated)
                && Instant.now().isBefore(notices)) {
            Thread.sleep(1_000);
            stats = sandboxStats();
        }
        System.out.printf("sandbox: %d refunds executed for %d payments (%d refunded more than once); "
                        + "%d escalation notices; %d breaks closed by a confirmed refund, %d escalated%n",
                stats.integer("refunds"), stats.integer("payments_refunded"),
                stats.integer("payments_refunded_more_than_once"), stats.integer("escalations"), refunded, escalated);
        assertThat(stats.integer("payments_refunded_more_than_once")).as("no payment refunded twice").isZero();
        assertThat(stats.integer("refunds")).as("one refund per break closed by a refund").isEqualTo(refunded);
        assertThat(stats.integer("escalations")).as("every escalation reached the ops sink")
                .isGreaterThanOrEqualTo(escalated);
    }

    @Test
    @Order(4)
    void anAmbiguousBatchLineIsReportedAsAmbiguousWithTwoWitnessesAndEscalated() throws InterruptedException {
        HttpResponse<String> r = Api.send(HttpRequest.newBuilder(Api.SANDBOX.resolve("/v1/inject?type=AMBIGUOUS_BATCH"
                + "&count=4")).POST(HttpRequest.BodyPublishers.noBody()));
        assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
        JsonObject report = StrictJson.parse(r.body().getBytes(StandardCharsets.UTF_8));
        String merchant = report.string("merchant_id");
        List<String> lines = Arrays.stream(report.string("batch_lines").split(";"))
                .map(k -> k.substring(k.indexOf('/') + 1)).toList();   // "m/fileId:lineNo" -> the break's subject
        assertThat(lines).as("two unreferenced lines").hasSize(2);
        Instant deadline = Instant.now().plus(DETECTION_LIMIT);
        Map<String, String> breakIds = new TreeMap<>();
        while (breakIds.size() < lines.size() && Instant.now().isBefore(deadline)) {
            Thread.sleep(2_000);
            for (Map<String, String> b : Api.breaks(Api.get("/v1/breaks?type=ORPHAN_SETTLEMENT&merchantId=" + merchant,
                    Api.POOL.opsToken()).body())) {
                if (lines.contains(b.get("subject_key")) && b.get("state").equals("ESCALATED")) {
                    breakIds.put(b.get("subject_key"), b.get("break_id"));
                }
            }
        }
        assertThat(breakIds.keySet()).as("each line raised as an orphan and escalated").containsExactlyInAnyOrderElementsOf(lines);
        for (Map.Entry<String, String> e : breakIds.entrySet()) {
            String detail = Api.get("/v1/breaks/" + e.getValue(), Api.POOL.opsToken()).body();
            assertThat(detail).as(e.getKey()).contains("\"reason\":\"AMBIGUOUS\"")
                    .contains("\"status\":\"AMBIGUOUS\"").contains("\"witnesses\":[[")
                    .contains("two subsets fit, for example");
            System.out.printf("ambiguous line %s/%s: escalated with its witnesses; %s%n", merchant, e.getKey(),
                    lastReason(e.getValue()));
        }
    }

    static boolean allHandled(TreeSet<String> expected, TreeMap<String, Map<String, String>> breaks) {
        return expected.stream().allMatch(k -> {
            Map<String, String> b = breaks.get(k);
            return b != null && HANDLED_AS.get(k.substring(0, k.indexOf(' '))).contains(b.get("state"));
        });
    }

    @Test
    @Order(3)
    void anEscalatedBreakIsResolvedByHandThroughTheAuditedApi() {
        Map<String, String> escalated = oneToOneBreaks().values().stream()
                .filter(b -> b.get("state").equals("ESCALATED")).findFirst()
                .orElseThrow(() -> new AssertionError("no escalated break to resolve"));
        String path = "/v1/breaks/" + escalated.get("break_id") + "/resolve";
        HttpRequest.BodyPublisher note = HttpRequest.BodyPublishers
                .ofString("{\"note\":\"e2e: checked with the bank by hand\"}");
        String key = "e2e-resolve-" + escalated.get("break_id");   // D94: every manual POST carries one
        HttpResponse<String> merchant = Api.send(Api.driftline(path).header("Idempotency-Key", key)
                .header("Authorization", "Bearer " + Api.POOL.merchants().get(0).token()).POST(note));
        assertThat(merchant.statusCode()).as("a merchant token has no breaks:resolve").isEqualTo(403);
        HttpResponse<String> done = Api.send(Api.driftline(path).header("Idempotency-Key", key)
                .header("Authorization", "Bearer " + Api.POOL.opsToken()).POST(note));
        assertThat(done.statusCode()).as(done.body()).isEqualTo(200);
        assertThat(done.body()).contains("\"state\":\"RESOLVED_MANUAL\"").contains("\"audit_seq\":");
        HttpResponse<String> repeat = Api.send(Api.driftline(path).header("Idempotency-Key", key)
                .header("Authorization", "Bearer " + Api.POOL.opsToken()).POST(note));
        assertThat(repeat.statusCode()).as("the same request again: the first answer, nothing done").isEqualTo(200);
        assertThat(repeat.headers().firstValue("Idempotent-Replayed")).contains("true");
        assertThat(repeat.body()).isEqualTo(done.body());
        HttpResponse<String> again = Api.send(Api.driftline(path).header("Idempotency-Key", key + "-2")
                .header("Authorization", "Bearer " + Api.POOL.opsToken()).POST(note));
        assertThat(again.statusCode()).as("a closed break cannot be closed again").isEqualTo(409);
        System.out.printf("resolved by hand: %s %s/%s -> %s%n", escalated.get("type"), escalated.get("merchant_id"),
                escalated.get("subject_key"), done.body().trim());
    }

    /** How many raised one-to-one breaks the world does not imply lack the evidence that heals them. */
    static long unhealedExtras(TreeMap<String, TreeMap<String, String>> raised, List<Injected> injected) {
        long n = 0;
        for (Injected i : injected) {
            for (Map.Entry<String, String> e : raised.getOrDefault(i.merchant(), new TreeMap<>()).entrySet()) {
                String healer = HEALED_BY.get(e.getKey().substring(0, e.getKey().indexOf(' ')));
                if (!i.expected().contains(e.getKey())
                        && (healer == null || !Arrays.asList(e.getValue().split(",")).contains(healer))) {
                    n++;
                }
            }
        }
        return n;
    }

    /**
     * {@code merchant → ("TYPE merchant/subject" → evidence keys)} of every one-to-one break, from the ops API with the
     * ops token.
     */
    static TreeMap<String, TreeMap<String, String>> raisedOneToOne() {
        HttpResponse<String> r = Api.get("/v1/breaks", Api.POOL.opsToken());
        assertThat(r.statusCode()).isEqualTo(200);
        TreeMap<String, TreeMap<String, String>> out = new TreeMap<>();
        for (Map<String, String> b : Api.breaks(r.body())) {
            if (ONE_TO_ONE.contains(b.get("type"))) {
                out.computeIfAbsent(b.get("merchant_id"), m -> new TreeMap<>())
                        .put(b.get("type") + " " + b.get("merchant_id") + "/" + b.get("subject_key"),
                                b.getOrDefault("evidence_keys", ""));
            }
        }
        return out;
    }

    // ---- ingest security ------------------------------------------------------------------------------------------

    static byte[] webhook(String eventId, String paymentId) {
        return GatewayWebhookCodec.encode(new GatewayWebhook(eventId, EventType.PAYMENT_SUCCEEDED,
                Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(Duration.ofHours(2)),
                new PaymentPayload(PaymentId.of(paymentId), OrderId.of("ord_" + paymentId), Money.ofPaise(12_345))));
    }

    static HttpResponse<String> postWebhook(BootstrapFile.Merchant m, byte[] body, String signature) {
        return Api.send(Api.driftline("/v1/webhooks/gateway/" + m.id().value())
                .header(WebhookSignature.HEADER, signature).POST(HttpRequest.BodyPublishers.ofByteArray(body)));
    }

    static String sign(BootstrapFile.Merchant m, Instant at, byte[] body) {
        return WebhookSignature.sign(m.secret().getBytes(StandardCharsets.US_ASCII), at, body);
    }

    @Test
    @Order(5)
    void aReplayedWebhookIsAcceptedAgainAndChangesNothing() throws InterruptedException {
        byte[] body = webhook("evt_e2e_replay", "pay_e2e_replay");
        HttpResponse<String> first = postWebhook(A, body, sign(A, Instant.now(), body));
        HttpResponse<String> replay = postWebhook(A, body, sign(A, Instant.now(), body));
        assertThat(first.statusCode()).isEqualTo(202);
        assertThat(replay.statusCode()).isEqualTo(202);
        assertThat(replay.body()).isEqualTo(first.body());
        // A marker sent after the replay on the same partition: once it is applied, the replay was consumed.
        byte[] marker = webhook("evt_e2e_marker", "pay_e2e_marker");
        assertThat(postWebhook(A, marker, sign(A, Instant.now(), marker)).statusCode()).isEqualTo(202);
        Instant deadline = Instant.now().plusSeconds(60);
        while (Api.get("/v1/payments/pay_e2e_marker/timeline", A.token()).statusCode() != 200) {
            assertThat(Instant.now()).as("the marker is applied within 60 s").isBefore(deadline);
            Thread.sleep(500);
        }
        String timeline = Api.get("/v1/payments/pay_e2e_replay/timeline", A.token()).body();
        assertThat(timeline.split("\"event_key\":\"evt_e2e_replay\"", -1)).as("one decision for two deliveries")
                .hasSize(2);
    }

    @Test
    @Order(6)
    void aBadSignatureIs401() {
        byte[] body = webhook("evt_e2e_bad", "pay_e2e_bad");
        String signature = sign(A, Instant.now(), body);
        byte[] tampered = Arrays.copyOf(body, body.length);
        tampered[tampered.length - 3] = (byte) '7';
        assertThat(postWebhook(A, tampered, signature).statusCode()).isEqualTo(401);
        assertThat(postWebhook(A, body, sign(B, Instant.now(), body)).statusCode()).as("another merchant's secret")
                .isEqualTo(401);
    }

    @Test
    @Order(7)
    void aStaleTimestampIsRejected() {
        byte[] body = webhook("evt_e2e_stale", "pay_e2e_stale");
        HttpResponse<String> stale = postWebhook(A, body, sign(A, Instant.now().minusSeconds(600), body));
        assertThat(stale.statusCode()).isEqualTo(401);
        assertThat(stale.body()).contains("OUTSIDE_REPLAY_WINDOW");
    }

    @Test
    @Order(8)
    void aCrossMerchantTokenIs403() {
        byte[] order = OrderEventCodec.encode(new OrderEventMessage(EventType.ORDER_CREATED, Instant.now().minusSeconds(60)
                .truncatedTo(ChronoUnit.SECONDS), new OrderPayload(OrderId.of("ord_e2e_x"), 1, Money.ofPaise(100),
                Instant.now().plusSeconds(900).truncatedTo(ChronoUnit.SECONDS), Optional.empty())));
        HttpResponse<String> r = Api.send(Api.driftline("/v1/merchants/" + B.id().value() + "/order-events")
                .header("Authorization", "Bearer " + A.token()).header("Idempotency-Key", "ord_e2e_x-v1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(order)));
        assertThat(r.statusCode()).isEqualTo(403);
        assertThat(Api.get("/v1/breaks?merchantId=" + B.id().value(), A.token()).statusCode()).isEqualTo(403);
        HttpResponse<String> own = Api.send(Api.driftline("/v1/merchants/" + A.id().value() + "/order-events")
                .header("Authorization", "Bearer " + A.token()).POST(HttpRequest.BodyPublishers.ofByteArray(order)));
        assertThat(own.statusCode()).as("and the Idempotency-Key is required").isEqualTo(400);
    }
}
