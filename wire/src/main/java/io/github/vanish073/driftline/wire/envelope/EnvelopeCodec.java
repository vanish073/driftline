package io.github.vanish073.driftline.wire.envelope;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventPayload;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.SettlementFileEndPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.WireTime;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.Optional;
import tools.jackson.core.JsonGenerator;

/**
 * The value of one Kafka record on {@code driftline.events.v1}: an {@link EventEnvelope} as strict JSON (DESIGN §4.2,
 * D46). Ingest and the settlement worker encode; the partition driver decodes. A record that does not decode is
 * quarantined by the matcher in the batch's own transaction (§5.7), never skipped.
 *
 * <pre>
 * {"schema_version":1,"merchant_id":"m_1","partition":3,"source":"GATEWAY","event_key":"evt_1",
 *  "event_type":"payment.succeeded","event_time":"2026-11-13T06:00:00.123Z","received_at":"…",
 *  "payload_sha256":"…64 hex…","traceparent":"00-…",          (traceparent only when present)
 *  "payload":{"payment_id":"pay_1","order_id":"ord_1","amount_paise":49900}}
 * </pre>
 *
 * The encoding is canonical (fixed field order, no optional field written as null), so {@code encode(decode(b))} equals
 * {@code b} for every {@code b} this class wrote. Decoding goes through the envelope's own constructor, so a record
 * that breaks the schema (a source the event type does not allow, a payload of the wrong shape) is rejected here.
 */
public final class EnvelopeCodec {

    private EnvelopeCodec() {
    }

    public static byte[] encode(EventEnvelope e) {
        return StrictJson.write(g -> {
            g.writeNumberProperty("schema_version", e.schemaVersion());
            g.writeStringProperty("merchant_id", e.merchantId().value());
            g.writeNumberProperty("partition", e.partition());
            g.writeStringProperty("source", e.source().name());
            g.writeStringProperty("event_key", e.eventKey());
            g.writeStringProperty("event_type", e.eventType().wireName());
            g.writeStringProperty("event_time", WireTime.format(e.eventTime()));
            g.writeStringProperty("received_at", WireTime.format(e.receivedAt()));
            g.writeStringProperty("payload_sha256", e.payloadSha256().hex());
            if (e.traceparent().isPresent()) {
                g.writeStringProperty("traceparent", e.traceparent().get());
            }
            g.writeName("payload");
            g.writeStartObject();
            writePayload(g, e.payload());
            g.writeEndObject();
        });
    }

    private static void writePayload(JsonGenerator g, EventPayload payload) {
        switch (payload) {
            case PaymentPayload p -> {
                g.writeStringProperty("payment_id", p.paymentId().value());
                g.writeStringProperty("order_id", p.orderId().value());
                g.writeNumberProperty("amount_paise", p.amount().paise());
            }
            case RefundPayload r -> {
                g.writeStringProperty("payment_id", r.paymentId().value());
                g.writeNumberProperty("amount_paise", r.amount().paise());
            }
            case OrderPayload o -> {
                g.writeStringProperty("order_id", o.orderId().value());
                g.writeNumberProperty("version", o.version());
                g.writeNumberProperty("amount_paise", o.amount().paise());
                g.writeStringProperty("expires_at", WireTime.format(o.expiresAt()));
                if (o.paidWith().isPresent()) {
                    g.writeStringProperty("paid_with", o.paidWith().get().value());
                }
            }
            case SettlementLinePayload l -> {
                g.writeStringProperty("file_id", l.fileId());
                g.writeNumberProperty("line_no", l.lineNo());
                g.writeStringProperty("settlement_date", l.settlementDate().toString());
                g.writeStringProperty("value_date", l.valueDate().toString());
                g.writeNumberProperty("amount_paise", l.amount().paise());
                if (l.fee().isPresent()) {
                    g.writeNumberProperty("fee_paise", l.fee().get().paise());
                }
                if (l.tax().isPresent()) {
                    g.writeNumberProperty("tax_paise", l.tax().get().paise());
                }
                if (l.reference().isPresent()) {
                    g.writeStringProperty("reference", l.reference().get());
                }
                if (l.batchId().isPresent()) {
                    g.writeStringProperty("batch_id", l.batchId().get());
                }
                g.writeStringProperty("narration", l.narration());
            }
            case SettlementFileEndPayload f -> {
                g.writeStringProperty("file_id", f.fileId());
                g.writeStringProperty("settlement_date", f.settlementDate().toString());
                g.writeNumberProperty("line_count", f.lineCount());
                g.writeNumberProperty("total_paise", f.totalAmount().paise());
                g.writeStringProperty("file_sha256", f.fileSha256().hex());
            }
        }
    }

    /** @throws MalformedJson if {@code value} is not a valid v1 envelope; the message names a field, never a value */
    public static EventEnvelope decode(byte[] value) {
        JsonObject root = StrictJson.parse(value);
        try {
            long version = root.integer("schema_version");
            if (version != EventEnvelope.SCHEMA_VERSION) {
                throw new MalformedJson("schema_version " + version + " is not supported");
            }
            EventType type = eventType(root.string("event_type"));
            String traceparent = root.optionalString("traceparent");
            return new EventEnvelope(EventEnvelope.SCHEMA_VERSION, MerchantId.of(root.string("merchant_id")),
                    Math.toIntExact(root.integer("partition")), source(root.string("source")), root.string("event_key"),
                    type, WireTime.parse("event_time", root.string("event_time")),
                    WireTime.parse("received_at", root.string("received_at")), new Sha256(root.string("payload_sha256")),
                    payload(type, root.object("payload")), Optional.ofNullable(traceparent));
        } catch (MalformedJson e) {
            throw e;
        } catch (IllegalArgumentException | ArithmeticException | DateTimeException e) {
            throw new MalformedJson("invalid envelope: " + e.getMessage());
        }
    }

    private static EventPayload payload(EventType type, JsonObject p) {
        return switch (type) {
            case PAYMENT_PENDING, PAYMENT_SUCCEEDED, PAYMENT_FAILED -> new PaymentPayload(
                    PaymentId.of(p.string("payment_id")), OrderId.of(p.string("order_id")),
                    Money.ofPaise(p.integer("amount_paise")));
            case REFUND_PROCESSED, REFUND_FAILED -> new RefundPayload(PaymentId.of(p.string("payment_id")),
                    Money.ofPaise(p.integer("amount_paise")));
            case ORDER_CREATED, ORDER_PENDING_PAYMENT, ORDER_PAID, ORDER_FULFILLED, ORDER_EXPIRED, ORDER_CANCELLED -> {
                String paidWith = p.optionalString("paid_with");
                yield new OrderPayload(OrderId.of(p.string("order_id")), p.integer("version"),
                        Money.ofPaise(p.integer("amount_paise")), WireTime.parse("payload.expires_at", p.string("expires_at")),
                        paidWith == null ? Optional.empty() : Optional.of(PaymentId.of(paidWith)));
            }
            case SETTLEMENT_LINE -> new SettlementLinePayload(p.string("file_id"),
                    Math.toIntExact(p.integer("line_no")), date(p, "settlement_date"), date(p, "value_date"),
                    Money.ofPaise(p.integer("amount_paise")), optionalPaise(p, "fee_paise"), optionalPaise(p, "tax_paise"),
                    Optional.ofNullable(p.optionalString("reference")), Optional.ofNullable(p.optionalString("batch_id")),
                    p.string("narration"));
            case SETTLEMENT_FILE_END -> new SettlementFileEndPayload(p.string("file_id"), date(p, "settlement_date"),
                    Math.toIntExact(p.integer("line_count")), Money.ofPaise(p.integer("total_paise")),
                    new Sha256(p.string("file_sha256")));
        };
    }

    private static Optional<Money> optionalPaise(JsonObject p, String field) {
        return p.has(field) ? Optional.of(Money.ofPaise(p.integer(field))) : Optional.empty();
    }

    private static LocalDate date(JsonObject p, String field) {
        try {
            return LocalDate.parse(p.string(field));
        } catch (DateTimeException e) {
            throw new MalformedJson("payload." + field + " is not an ISO-8601 date");
        }
    }

    private static EventType eventType(String wireName) {
        try {
            return EventType.fromWireName(wireName);
        } catch (IllegalArgumentException e) {
            throw new MalformedJson("event_type is not a known event type");
        }
    }

    private static Source source(String name) {
        for (Source s : Source.values()) {
            if (s.name().equals(name)) {
                return s;
            }
        }
        throw new MalformedJson("source is not a known source");
    }
}
