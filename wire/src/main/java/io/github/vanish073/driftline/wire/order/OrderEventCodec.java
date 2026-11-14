package io.github.vanish073.driftline.wire.order;

import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.WireTime;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.util.Optional;

/**
 * Encodes and decodes {@link OrderEventMessage}. The body is:
 *
 * <pre>
 * {"type":"order.paid","occurred_at":"2026-11-13T06:00:00.123Z",
 *  "order":{"id":"ord_…","version":3,"amount_paise":49900,"currency":"INR",
 *           "expires_at":"2026-11-13T06:15:00Z","paid_with":"pay_…"}}
 * </pre>
 */
public final class OrderEventCodec {

    private OrderEventCodec() {
    }

    public static byte[] encode(OrderEventMessage message) {
        OrderPayload o = message.order();
        return StrictJson.write(g -> {
            g.writeStringProperty("type", message.type().wireName());
            g.writeStringProperty("occurred_at", WireTime.format(message.occurredAt()));
            g.writeName("order");
            g.writeStartObject();
            g.writeStringProperty("id", o.orderId().value());
            g.writeNumberProperty("version", o.version());
            g.writeNumberProperty("amount_paise", o.amount().paise());
            g.writeStringProperty("currency", "INR");
            g.writeStringProperty("expires_at", WireTime.format(o.expiresAt()));
            if (o.paidWith().isPresent()) {
                g.writeStringProperty("paid_with", o.paidWith().get().value());
            }
            g.writeEndObject();
        });
    }

    /** @throws MalformedJson if the body is not a valid order event */
    public static OrderEventMessage decode(byte[] body) {
        JsonObject root = StrictJson.parse(body);
        EventType type;
        try {
            type = EventType.fromWireName(root.string("type"));
        } catch (IllegalArgumentException e) {
            throw new MalformedJson("type is not a known event type");
        }
        JsonObject o = root.object("order");
        if (!"INR".equals(o.string("currency"))) {
            throw new MalformedJson("currency must be INR (DESIGN §1)");
        }
        try {
            String paidWith = o.optionalString("paid_with");
            OrderPayload order = new OrderPayload(OrderId.of(o.string("id")), o.integer("version"),
                    Money.ofPaise(o.integer("amount_paise")), WireTime.parse("order.expires_at", o.string("expires_at")),
                    paidWith == null ? Optional.empty() : Optional.of(PaymentId.of(paidWith)));
            return new OrderEventMessage(type, WireTime.parse("occurred_at", root.string("occurred_at")), order);
        } catch (MalformedJson e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new MalformedJson("invalid order event: " + e.getMessage());
        }
    }
}
