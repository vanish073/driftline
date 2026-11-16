package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.wire.call.CallWire;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** The dispatcher's HTTP calls (D65): both ends read what the other wrote, and only an answer is a result. */
class CallWireTest {

    private static final MerchantId M = MerchantId.of("m_1");
    private static final PaymentId P = PaymentId.of("pay_1");
    private static final OrderId O = OrderId.of("ord_1");
    private static final Money AMOUNT = Money.ofPaise(49_900);
    private static final Instant AT = Instant.parse("2026-11-13T06:00:00.123Z");

    private static OutboundCall call(CallTarget target, Optional<String> key, CallRequest request) {
        return new OutboundCall("cmd_x.1", M, target, key, request);
    }

    private static List<OutboundCall> everyCall() {
        return List.of(
                call(CallTarget.GATEWAY, Optional.empty(), new CallRequest.StatusQuery(P)),
                call(CallTarget.GATEWAY, Optional.empty(), new CallRequest.StatusQueryOrder(O)),
                call(CallTarget.GATEWAY, Optional.of("rfd_key"), new CallRequest.Refund(P, O, AMOUNT)),
                call(CallTarget.GATEWAY, Optional.empty(), new CallRequest.RefundLookup(P)),
                call(CallTarget.MERCHANT, Optional.of("cfm_key"), new CallRequest.MerchantConfirm(O, P, AMOUNT)),
                call(CallTarget.OPS, Optional.empty(),
                        new CallRequest.EscalationNotify("brk_1", BreakType.SHORT_SETTLEMENT, "m_1/stl:3", "short by 1.00")));
    }

    @Test
    void theServerReadsEveryRequestTheClientWrites() {
        for (OutboundCall c : everyCall()) {
            CallWire.HttpCall http = CallWire.request(c);
            assertThat(CallWire.parse(c.target(), http.method(), http.path(), http.body()))
                    .as(c.request().getClass().getSimpleName())
                    .contains(new CallWire.Incoming(M, c.request()));
        }
    }

    @Test
    void aRefundCarriesItsKeyInTheBodyAsWell() {
        CallWire.HttpCall http = CallWire.request(everyCall().get(2));
        assertThat(http.method()).isEqualTo("POST");
        assertThat(http.path()).isEqualTo("/v1/merchants/m_1/payments/pay_1/refunds");
        assertThat(new String(http.body(), StandardCharsets.UTF_8))
                .isEqualTo("{\"order_id\":\"ord_1\",\"amount_paise\":49900,"
                        + "\"metadata\":{\"driftline_refund_key\":\"rfd_key\"}}");
    }

    @Test
    void theClientDecodesEveryReplyIntoTheFactsTheWorldAnsweredWith() {
        List<TreeMap<String, String>> answers = List.of(
                kv("status", "SUCCEEDED", "order_id", "ord_1", "amount", "499.00", "at", AT.toString()),
                kv("count", "2", "payment_1", "pay_1", "status_1", "FAILED", "amount_1", "499.00", "at_1", AT.toString(),
                        "payment_2", "pay_2", "status_2", "SUCCEEDED", "amount_2", "499.00", "at_2", AT.toString()),
                kv("status", "PROCESSED", "refund_id", "rfd_9", "processed_at", AT.toString()),
                kv("found", "yes", "refund_id", "rfd_9", "processed_at", AT.toString()),
                kv("status", "REJECTED"),
                kv("status", "DELIVERED"));
        List<OutboundCall> calls = everyCall();
        for (int i = 0; i < calls.size(); i++) {
            CallRequest r = calls.get(i).request();
            CallWire.Reply reply = CallWire.encodeReply(r, answers.get(i));
            assertThat(CallWire.reply(r, reply.status(), reply.body())).as(r.getClass().getSimpleName())
                    .contains(answers.get(i));
        }
    }

    @Test
    void theNegativeAnswersRoundTripToo() {
        CallRequest.StatusQuery q = new CallRequest.StatusQuery(P);
        CallWire.Reply unknown = CallWire.encodeReply(q, kv("status", "UNKNOWN"));
        assertThat(unknown.status()).isEqualTo(404);
        assertThat(CallWire.reply(q, unknown.status(), unknown.body())).contains(kv("status", "UNKNOWN"));
        CallRequest.StatusQueryOrder none = new CallRequest.StatusQueryOrder(O);
        CallWire.Reply empty = CallWire.encodeReply(none, kv("count", "0"));
        assertThat(CallWire.reply(none, empty.status(), empty.body())).contains(kv("count", "0"));
        CallRequest.RefundLookup l = new CallRequest.RefundLookup(P);
        CallWire.Reply notFound = CallWire.encodeReply(l, kv("found", "no"));
        assertThat(CallWire.reply(l, notFound.status(), notFound.body())).contains(kv("found", "no"));
        CallRequest.Refund r = new CallRequest.Refund(P, O, AMOUNT);
        CallWire.Reply failed = CallWire.encodeReply(r, kv("status", "FAILED"));
        assertThat(CallWire.reply(r, failed.status(), failed.body())).contains(kv("status", "FAILED"));
    }

    @Test
    void theLookupReportsTheEarliestRefundTheGatewayShows() {
        byte[] body = ("{\"payment_id\":\"pay_1\",\"refunds\":{\"rfd_b\":{\"processed_at\":\"2026-11-13T06:00:00Z\"},"
                + "\"rfd_a\":{\"processed_at\":\"2026-11-13T07:00:00Z\"}}}").getBytes(StandardCharsets.UTF_8);
        assertThat(CallWire.reply(new CallRequest.RefundLookup(P), 200, body)).contains(
                kv("found", "yes", "refund_id", "rfd_b", "processed_at", "2026-11-13T06:00:00Z"));
    }

    @Test
    void anythingButAnAnswerIsNoResultSoTheCallIsRetriedWithTheSameKey() {
        CallRequest.Refund r = new CallRequest.Refund(P, O, AMOUNT);
        byte[] processed = CallWire.encodeReply(r, kv("status", "PROCESSED", "refund_id", "rfd_9",
                "processed_at", AT.toString())).body();
        for (int status : new int[] {201, 400, 404, 409, 422, 429, 500, 502, 503}) {
            assertThat(CallWire.reply(r, status, processed)).as("refund answered %d", status).isEmpty();
        }
        assertThat(CallWire.reply(r, 200, "not json".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(CallWire.reply(r, 200, "{\"status\":\"MAYBE\"}".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(CallWire.reply(new CallRequest.StatusQuery(P), 200,
                "{\"status\":\"WEIRD\"}".getBytes(StandardCharsets.UTF_8))).isEmpty();
        assertThat(CallWire.reply(new CallRequest.StatusQuery(P), 503, new byte[0])).isEmpty();
        CallRequest.Observation o = new CallRequest.Observation(EventType.REFUND_PROCESSED, "obs_1", AT,
                new RefundPayload(P, AMOUNT));
        assertThat(CallWire.reply(o, 200, new byte[0])).as("ingest answers 202 after the Kafka ack").isEmpty();
        assertThat(CallWire.reply(o, 202, new byte[0])).contains(kv("status", "ACCEPTED"));
    }

    @Test
    void anObservationRoundTripsWithEitherPayload() {
        for (CallRequest.Observation o : List.of(
                new CallRequest.Observation(EventType.PAYMENT_SUCCEEDED, "obs_p", AT, new PaymentPayload(P, O, AMOUNT)),
                new CallRequest.Observation(EventType.REFUND_PROCESSED, "obs_r", AT, new RefundPayload(P, AMOUNT)))) {
            CallWire.HttpCall http = CallWire.request(call(CallTarget.DRIFTLINE, Optional.empty(), o));
            assertThat(http.path()).isEqualTo(CallWire.OBSERVATIONS_PATH);
            assertThat(CallWire.parseObservation(http.body())).isEqualTo(new CallWire.Incoming(M, o));
        }
        assertThatThrownBy(() -> CallWire.parseObservation(("{\"merchant_id\":\"m_1\",\"type\":\"refund.processed\","
                + "\"event_key\":\"obs_r\",\"occurred_at\":\"2026-11-13T06:00:00Z\"}").getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(MalformedJson.class);
        assertThatThrownBy(() -> CallWire.parseObservation(("{\"merchant_id\":\"m_1\",\"type\":\"order.paid\","
                + "\"event_key\":\"obs_r\",\"occurred_at\":\"2026-11-13T06:00:00Z\","
                + "\"refund\":{\"payment_id\":\"pay_1\",\"amount_paise\":1}}").getBytes(StandardCharsets.UTF_8)))
                .as("a payload that does not fit the type").isInstanceOf(MalformedJson.class);
    }

    @Test
    void anUnknownRouteIsNoRequestAndABadBodyIsMalformed() {
        assertThat(CallWire.parse(CallTarget.GATEWAY, "DELETE", "/v1/merchants/m_1/payments/pay_1", new byte[0])).isEmpty();
        assertThat(CallWire.parse(CallTarget.GATEWAY, "GET", "/v2/merchants/m_1/payments/pay_1", new byte[0])).isEmpty();
        assertThat(CallWire.parse(CallTarget.MERCHANT, "GET", "/v1/merchants/m_1/payments/pay_1", new byte[0])).isEmpty();
        assertThatThrownBy(() -> CallWire.parse(CallTarget.GATEWAY, "GET", "/v1/merchants/m 1/payments/pay_1",
                new byte[0])).isInstanceOf(MalformedJson.class);
        assertThatThrownBy(() -> CallWire.parse(CallTarget.GATEWAY, "POST", "/v1/merchants/m_1/payments/pay_1/refunds",
                "{\"order_id\":\"ord_1\"}".getBytes(StandardCharsets.UTF_8))).isInstanceOf(MalformedJson.class);
    }

    private static TreeMap<String, String> kv(String... pairs) {
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(pairs[i], pairs[i + 1]);
        }
        return out;
    }
}
