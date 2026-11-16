package io.github.vanish073.driftline.wire.call;

import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventPayload;
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
import io.github.vanish073.driftline.wire.WireTime;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The HTTP shape of every dispatcher call (DESIGN §9.1, D65), in one place for both ends: the app encodes requests and
 * decodes replies into the same facts the simulator's world models answer with, and the sandbox parses requests and
 * encodes those facts as replies. Paths are relative to the target's base URI. Ids are tokens of
 * {@code [A-Za-z0-9._-]}, so they are safe as path segments.
 *
 * <pre>
 * GATEWAY  GET  /v1/merchants/{m}/payments/{p}            → 200 {"id","order_id","status","amount_paise","updated_at"}
 *                                                           404 when the gateway does not know the payment
 *          GET  /v1/merchants/{m}/orders/{o}/payments     → 200 {"order_id","payments":{"{p}":{"status","amount_paise","updated_at"}}}
 *          POST /v1/merchants/{m}/payments/{p}/refunds    {"order_id","amount_paise","metadata":{"driftline_refund_key"}}
 *                                                         → 200 {"status":"PROCESSED","refund_id","processed_at"} or {"status":"FAILED"}
 *          GET  /v1/merchants/{m}/payments/{p}/refunds    → 200 {"payment_id","refunds":{"{refundId}":{"processed_at"}}}
 * MERCHANT POST /v1/merchants/{m}/orders/{o}/confirm      {"payment_id","amount_paise"} → 200 {"status":"ACCEPTED"|"REJECTED"}
 * DRIFTLINE POST /internal/v1/observations                {"merchant_id","type","event_key","occurred_at",
 *                                                          "payment":{"id","order_id","amount_paise"} | "refund":{"payment_id","amount_paise"}}
 *                                                         → 202 {"accepted","key"}
 * OPS      POST /v1/escalations                           {"merchant_id","break_id","type","subject","reason"} → 2xx
 * </pre>
 *
 * <b>Only an answer is a result.</b> A reply this class cannot read, or any status not listed above (a 5xx, a 409 for a
 * key in use, a 429), decodes to no result: the dispatcher's lease runs out and the call is retried with the same key.
 * A refund is never inferred as FAILED from an HTTP status.
 */
public final class CallWire {

    public static final String OBSERVATIONS_PATH = "/internal/v1/observations";
    public static final String ESCALATIONS_PATH = "/v1/escalations";

    /** One HTTP request, relative to the target's base URI; {@code body} is empty for a GET. */
    public record HttpCall(String method, String path, byte[] body) {
        public HttpCall {
            Objects.requireNonNull(method, "method");
            Objects.requireNonNull(path, "path");
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }

    /** A request as the server end reads it. */
    public record Incoming(MerchantId merchantId, CallRequest request) {
        public Incoming {
            Objects.requireNonNull(merchantId, "merchantId");
            Objects.requireNonNull(request, "request");
        }
    }

    /** A reply as the server end sends it. */
    public record Reply(int status, byte[] body) {
        public Reply {
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }

    private CallWire() {
    }

    // ---- the client end (Driftline's dispatcher) ----

    public static HttpCall request(OutboundCall call) {
        String m = "/v1/merchants/" + call.merchantId().value();
        return switch (call.request()) {
            case CallRequest.StatusQuery q -> get(m + "/payments/" + q.paymentId().value());
            case CallRequest.StatusQueryOrder q -> get(m + "/orders/" + q.orderId().value() + "/payments");
            case CallRequest.RefundLookup l -> get(m + "/payments/" + l.paymentId().value() + "/refunds");
            case CallRequest.Refund r -> new HttpCall("POST", m + "/payments/" + r.paymentId().value() + "/refunds",
                    StrictJson.write(g -> {
                        g.writeStringProperty("order_id", r.orderId().value());
                        g.writeNumberProperty("amount_paise", r.amount().paise());
                        if (call.idempotencyKey().isPresent()) {
                            g.writeName("metadata");
                            g.writeStartObject();
                            g.writeStringProperty("driftline_refund_key", call.idempotencyKey().get());
                            g.writeEndObject();
                        }
                    }));
            case CallRequest.MerchantConfirm c -> new HttpCall("POST", m + "/orders/" + c.orderId().value() + "/confirm",
                    StrictJson.write(g -> {
                        g.writeStringProperty("payment_id", c.paymentId().value());
                        g.writeNumberProperty("amount_paise", c.amount().paise());
                    }));
            case CallRequest.Observation o -> new HttpCall("POST", OBSERVATIONS_PATH, observation(call.merchantId(), o));
            case CallRequest.EscalationNotify e -> new HttpCall("POST", ESCALATIONS_PATH, StrictJson.write(g -> {
                g.writeStringProperty("merchant_id", call.merchantId().value());
                g.writeStringProperty("break_id", e.breakId());
                g.writeStringProperty("type", e.type().name());
                g.writeStringProperty("subject", e.subject());
                g.writeStringProperty("reason", e.reason());
            }));
        };
    }

    /** The facts a reply carries, or empty if it is not a result (see the class comment). */
    public static Optional<TreeMap<String, String>> reply(CallRequest request, int status, byte[] body) {
        try {
            return switch (request) {
                case CallRequest.StatusQuery q -> status == 404 ? Optional.of(kv("status", "UNKNOWN"))
                        : status == 200 ? paymentStatus(StrictJson.parse(body)) : Optional.empty();
                case CallRequest.StatusQueryOrder q -> status == 404 ? Optional.of(kv("count", "0"))
                        : status == 200 ? orderPayments(StrictJson.parse(body)) : Optional.empty();
                case CallRequest.Refund r -> status == 200 ? refund(StrictJson.parse(body)) : Optional.empty();
                case CallRequest.RefundLookup l -> status == 200 ? lookup(StrictJson.parse(body)) : Optional.empty();
                case CallRequest.MerchantConfirm c -> status == 200 ? confirm(StrictJson.parse(body)) : Optional.empty();
                case CallRequest.Observation o -> status == 202 ? Optional.of(kv("status", "ACCEPTED")) : Optional.empty();
                case CallRequest.EscalationNotify e -> status >= 200 && status < 300
                        ? Optional.of(kv("status", "DELIVERED")) : Optional.empty();
            };
        } catch (IllegalArgumentException | ArithmeticException unreadable) {   // MalformedJson included
            return Optional.empty();
        }
    }

    private static Optional<TreeMap<String, String>> paymentStatus(JsonObject p) {
        String status = p.string("status");
        if (!List.of("SUCCEEDED", "FAILED", "PENDING").contains(status)) {
            return Optional.empty();
        }
        return Optional.of(kv("status", status, "order_id", OrderId.of(p.string("order_id")).value(),
                "amount", Money.ofPaise(p.integer("amount_paise")).toString(),
                "at", WireTime.parse("updated_at", p.string("updated_at")).toString()));
    }

    private static Optional<TreeMap<String, String>> orderPayments(JsonObject root) {
        TreeMap<String, String> out = new TreeMap<>();
        int i = 0;
        for (Map.Entry<String, Object> e : root.object("payments").fields().entrySet()) {
            if (!(e.getValue() instanceof JsonObject p)) {
                return Optional.empty();
            }
            String status = p.string("status");
            if (!List.of("SUCCEEDED", "FAILED", "PENDING").contains(status)) {
                return Optional.empty();
            }
            i++;
            out.put("payment_" + i, PaymentId.of(e.getKey()).value());
            out.put("status_" + i, status);
            out.put("amount_" + i, Money.ofPaise(p.integer("amount_paise")).toString());
            out.put("at_" + i, WireTime.parse("updated_at", p.string("updated_at")).toString());
        }
        out.put("count", Integer.toString(i));
        return Optional.of(out);
    }

    private static Optional<TreeMap<String, String>> refund(JsonObject r) {
        String status = r.string("status");
        if (status.equals("PROCESSED")) {
            return Optional.of(kv("status", status, "refund_id", r.string("refund_id"),
                    "processed_at", WireTime.parse("processed_at", r.string("processed_at")).toString()));
        }
        return status.equals("FAILED") ? Optional.of(kv("status", status)) : Optional.empty();
    }

    /** The earliest refund the gateway shows (by processing time, then id), as the simulator's lookup answers. */
    private static Optional<TreeMap<String, String>> lookup(JsonObject root) {
        record Seen(String refundId, Instant at) {
        }
        List<Seen> seen = new ArrayList<>();
        for (Map.Entry<String, Object> e : root.object("refunds").fields().entrySet()) {
            if (!(e.getValue() instanceof JsonObject r)) {
                return Optional.empty();
            }
            seen.add(new Seen(e.getKey(), WireTime.parse("processed_at", r.string("processed_at"))));
        }
        Optional<Seen> first = seen.stream().min(Comparator.comparing(Seen::at).thenComparing(Seen::refundId));
        TreeMap<String, String> out = kv("found", first.isPresent() ? "yes" : "no");
        first.ifPresent(s -> {
            out.put("refund_id", s.refundId());
            out.put("processed_at", s.at().toString());
        });
        return Optional.of(out);
    }

    private static Optional<TreeMap<String, String>> confirm(JsonObject c) {
        String status = c.string("status");
        return status.equals("ACCEPTED") || status.equals("REJECTED") ? Optional.of(kv("status", status))
                : Optional.empty();
    }

    // ---- the observation body, read by Driftline's ingest ----

    static byte[] observation(MerchantId merchant, CallRequest.Observation o) {
        return StrictJson.write(g -> {
            g.writeStringProperty("merchant_id", merchant.value());
            g.writeStringProperty("type", o.eventType().wireName());
            g.writeStringProperty("event_key", o.eventKey());
            g.writeStringProperty("occurred_at", WireTime.format(o.eventTime()));
            switch (o.payload()) {
                case PaymentPayload p -> {
                    g.writeName("payment");
                    g.writeStartObject();
                    g.writeStringProperty("id", p.paymentId().value());
                    g.writeStringProperty("order_id", p.orderId().value());
                    g.writeNumberProperty("amount_paise", p.amount().paise());
                    g.writeEndObject();
                }
                case RefundPayload r -> {
                    g.writeName("refund");
                    g.writeStartObject();
                    g.writeStringProperty("payment_id", r.paymentId().value());
                    g.writeNumberProperty("amount_paise", r.amount().paise());
                    g.writeEndObject();
                }
                default -> throw new IllegalArgumentException("an observation carries a payment or a refund");
            }
        });
    }

    /** {@code POST /internal/v1/observations}. @throws MalformedJson if the body is not an observation */
    public static Incoming parseObservation(byte[] body) {
        JsonObject root = StrictJson.parse(body);
        try {
            EventType type = EventType.fromWireName(root.string("type"));
            EventPayload payload;
            if (root.has("payment") == root.has("refund")) {
                throw new MalformedJson("an observation carries exactly one of payment and refund");
            }
            if (root.has("payment")) {
                JsonObject p = root.object("payment");
                payload = new PaymentPayload(PaymentId.of(p.string("id")), OrderId.of(p.string("order_id")),
                        Money.ofPaise(p.integer("amount_paise")));
            } else {
                JsonObject r = root.object("refund");
                payload = new RefundPayload(PaymentId.of(r.string("payment_id")), Money.ofPaise(r.integer("amount_paise")));
            }
            return new Incoming(MerchantId.of(root.string("merchant_id")), new CallRequest.Observation(type,
                    root.string("event_key"), WireTime.parse("occurred_at", root.string("occurred_at")), payload));
        } catch (MalformedJson e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new MalformedJson("invalid observation: " + e.getMessage());
        }
    }

    // ---- the server end (the sandbox's gateway, merchant and ops sink) ----

    /**
     * Reads a request to {@code target} at {@code path} (relative to the target's base); empty if no route matches.
     *
     * @throws MalformedJson if a route matches but the body is not valid for it
     */
    public static Optional<Incoming> parse(CallTarget target, String method, String path, byte[] body) {
        String[] s = path.split("/", -1);
        // "", "v1", "merchants", m, ...
        try {
            if (target == CallTarget.OPS) {
                if (!method.equals("POST") || !path.equals(ESCALATIONS_PATH)) {
                    return Optional.empty();
                }
                JsonObject e = StrictJson.parse(body);
                return Optional.of(new Incoming(MerchantId.of(e.string("merchant_id")), new CallRequest.EscalationNotify(
                        e.string("break_id"), BreakType.valueOf(e.string("type")), e.string("subject"),
                        e.string("reason"))));
            }
            if (s.length < 6 || !s[0].isEmpty() || !s[1].equals("v1") || !s[2].equals("merchants")) {
                return Optional.empty();
            }
            MerchantId m = MerchantId.of(s[3]);
            String route = method + " " + s[4] + (s.length == 6 ? "" : "/" + s[6]) + "#" + s.length;
            CallRequest request = switch (target) {
                case GATEWAY -> switch (route) {
                    case "GET payments#6" -> new CallRequest.StatusQuery(PaymentId.of(s[5]));
                    case "GET orders/payments#7" -> new CallRequest.StatusQueryOrder(OrderId.of(s[5]));
                    case "GET payments/refunds#7" -> new CallRequest.RefundLookup(PaymentId.of(s[5]));
                    case "POST payments/refunds#7" -> {
                        JsonObject r = StrictJson.parse(body);
                        yield new CallRequest.Refund(PaymentId.of(s[5]), OrderId.of(r.string("order_id")),
                                Money.ofPaise(r.integer("amount_paise")));
                    }
                    default -> null;
                };
                case MERCHANT -> {
                    if (!route.equals("POST orders/confirm#7")) {
                        yield null;
                    }
                    JsonObject c = StrictJson.parse(body);
                    yield new CallRequest.MerchantConfirm(OrderId.of(s[5]), PaymentId.of(c.string("payment_id")),
                            Money.ofPaise(c.integer("amount_paise")));
                }
                default -> null;
            };
            return Optional.ofNullable(request).map(r -> new Incoming(m, r));
        } catch (MalformedJson e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new MalformedJson("invalid request: " + e.getMessage());
        }
    }

    /** The reply for the facts a world model answered with: the inverse of {@link #reply}. */
    public static Reply encodeReply(CallRequest request, Map<String, String> facts) {
        return switch (request) {
            case CallRequest.StatusQuery q -> facts.get("status").equals("UNKNOWN")
                    ? new Reply(404, StrictJson.write(g -> g.writeStringProperty("error", "unknown payment")))
                    : new Reply(200, StrictJson.write(g -> {
                        g.writeStringProperty("id", q.paymentId().value());
                        g.writeStringProperty("order_id", facts.get("order_id"));
                        g.writeStringProperty("status", facts.get("status"));
                        g.writeNumberProperty("amount_paise", Money.parse(facts.get("amount")).paise());
                        g.writeStringProperty("updated_at", WireTime.format(Instant.parse(facts.get("at"))));
                    }));
            case CallRequest.StatusQueryOrder q -> new Reply(200, StrictJson.write(g -> {
                g.writeStringProperty("order_id", q.orderId().value());
                g.writeName("payments");
                g.writeStartObject();
                int count = Integer.parseInt(facts.get("count"));
                for (int i = 1; i <= count; i++) {
                    g.writeName(facts.get("payment_" + i));
                    g.writeStartObject();
                    g.writeStringProperty("status", facts.get("status_" + i));
                    g.writeNumberProperty("amount_paise", Money.parse(facts.get("amount_" + i)).paise());
                    g.writeStringProperty("updated_at", WireTime.format(Instant.parse(facts.get("at_" + i))));
                    g.writeEndObject();
                }
                g.writeEndObject();
            }));
            case CallRequest.Refund r -> new Reply(200, StrictJson.write(g -> {
                g.writeStringProperty("status", facts.get("status"));
                if (facts.containsKey("refund_id")) {
                    g.writeStringProperty("refund_id", facts.get("refund_id"));
                    g.writeStringProperty("processed_at", WireTime.format(Instant.parse(facts.get("processed_at"))));
                }
            }));
            case CallRequest.RefundLookup l -> new Reply(200, StrictJson.write(g -> {
                g.writeStringProperty("payment_id", l.paymentId().value());
                g.writeName("refunds");
                g.writeStartObject();
                if ("yes".equals(facts.get("found"))) {
                    g.writeName(facts.get("refund_id"));
                    g.writeStartObject();
                    g.writeStringProperty("processed_at", WireTime.format(Instant.parse(facts.get("processed_at"))));
                    g.writeEndObject();
                }
                g.writeEndObject();
            }));
            case CallRequest.MerchantConfirm c ->
                    new Reply(200, StrictJson.write(g -> g.writeStringProperty("status", facts.get("status"))));
            case CallRequest.Observation o -> new Reply(202, StrictJson.write(g -> {
                g.writeBooleanProperty("accepted", true);
                g.writeStringProperty("key", o.eventKey());
            }));
            case CallRequest.EscalationNotify e ->
                    new Reply(202, StrictJson.write(g -> g.writeBooleanProperty("accepted", true)));
        };
    }

    // ---- helpers ----

    private static HttpCall get(String path) {
        return new HttpCall("GET", path, new byte[0]);
    }

    private static TreeMap<String, String> kv(String... pairs) {
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put(pairs[i], pairs[i + 1]);
        }
        return out;
    }
}
