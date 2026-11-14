package io.github.vanish073.driftline.wire.webhook;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.WireTime;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.json.StrictJson;

/**
 * Encodes and decodes {@link GatewayWebhook}. The body is:
 *
 * <pre>
 * {"id":"evt_…","type":"payment.succeeded","occurred_at":"2026-11-13T06:00:00.123Z",
 *  "payment":{"id":"pay_…","order_id":"ord_…","amount_paise":49900,"currency":"INR"}}
 * {"id":"evt_…","type":"refund.processed","occurred_at":"…",
 *  "refund":{"payment_id":"pay_…","amount_paise":49900,"currency":"INR"}}
 * </pre>
 *
 * Unknown fields are ignored, as gateways add fields over time; required ones are checked strictly. The encoding is
 * canonical, so {@code encode(decode(body))} is the form that is hashed into {@code payloadSha256}.
 */
public final class GatewayWebhookCodec {

    private GatewayWebhookCodec() {
    }

    public static byte[] encode(GatewayWebhook webhook) {
        return StrictJson.write(g -> {
            g.writeStringProperty("id", webhook.eventId());
            g.writeStringProperty("type", webhook.type().wireName());
            g.writeStringProperty("occurred_at", WireTime.format(webhook.occurredAt()));
            if (webhook.payload() instanceof PaymentPayload p) {
                g.writeName("payment");
                g.writeStartObject();
                g.writeStringProperty("id", p.paymentId().value());
                g.writeStringProperty("order_id", p.orderId().value());
                g.writeNumberProperty("amount_paise", p.amount().paise());
                g.writeStringProperty("currency", "INR");
                g.writeEndObject();
            } else if (webhook.payload() instanceof RefundPayload r) {
                g.writeName("refund");
                g.writeStartObject();
                g.writeStringProperty("payment_id", r.paymentId().value());
                g.writeNumberProperty("amount_paise", r.amount().paise());
                g.writeStringProperty("currency", "INR");
                g.writeEndObject();
            }
        });
    }

    /** @throws MalformedJson if the body is not a valid v1 webhook */
    public static GatewayWebhook decode(byte[] body) {
        JsonObject root = StrictJson.parse(body);
        EventType type = eventType(root.string("type"));
        String id = root.string("id");
        try {
            return switch (type) {
                case PAYMENT_PENDING, PAYMENT_SUCCEEDED, PAYMENT_FAILED -> {
                    JsonObject p = root.object("payment");
                    requireInr(p);
                    yield new GatewayWebhook(id, type, WireTime.parse("occurred_at", root.string("occurred_at")),
                            new PaymentPayload(PaymentId.of(p.string("id")), OrderId.of(p.string("order_id")),
                                    Money.ofPaise(p.integer("amount_paise"))));
                }
                case REFUND_PROCESSED, REFUND_FAILED -> {
                    JsonObject r = root.object("refund");
                    requireInr(r);
                    yield new GatewayWebhook(id, type, WireTime.parse("occurred_at", root.string("occurred_at")),
                            new RefundPayload(PaymentId.of(r.string("payment_id")), Money.ofPaise(r.integer("amount_paise"))));
                }
                default -> throw new MalformedJson("type " + type.wireName() + " is not a gateway event");
            };
        } catch (MalformedJson e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new MalformedJson("invalid webhook: " + e.getMessage());
        }
    }

    static EventType eventType(String wireName) {
        try {
            return EventType.fromWireName(wireName);
        } catch (IllegalArgumentException e) {
            throw new MalformedJson("type is not a known event type");
        }
    }

    static void requireInr(JsonObject o) {
        if (!"INR".equals(o.string("currency"))) {
            throw new MalformedJson("currency must be INR (DESIGN §1)");
        }
    }
}
