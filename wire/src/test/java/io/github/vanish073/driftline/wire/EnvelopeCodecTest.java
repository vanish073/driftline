package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.github.vanish073.driftline.wire.envelope.EnvelopeCodec;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

/** The Kafka record value (D46): canonical, lossless for every event type, and strict on the way in. */
class EnvelopeCodecTest {

    private static final Instant AT = Instant.parse("2026-11-13T06:00:00.123456Z");
    private static final Sha256 SHA = Sha256.of("x".getBytes(StandardCharsets.UTF_8));
    private static final String TRACE = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private static EventEnvelope envelope(Source source, EventType type, EventPayload payload, Optional<String> trace) {
        return new EventEnvelope(EventEnvelope.SCHEMA_VERSION, MerchantId.of("m_1"), 3, source, "key-1", type, AT,
                AT.plusSeconds(1), SHA, payload, trace);
    }

    private static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void aPaymentEnvelopeHasTheDocumentedCanonicalForm() {
        EventEnvelope e = envelope(Source.GATEWAY, EventType.PAYMENT_SUCCEEDED,
                new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(49_900)), Optional.of(TRACE));
        assertThat(utf8(EnvelopeCodec.encode(e))).isEqualTo("{\"schema_version\":1,\"merchant_id\":\"m_1\",\"partition\":3,"
                + "\"source\":\"GATEWAY\",\"event_key\":\"key-1\",\"event_type\":\"payment.succeeded\","
                + "\"event_time\":\"2026-11-13T06:00:00.123456Z\",\"received_at\":\"2026-11-13T06:00:01.123456Z\","
                + "\"payload_sha256\":\"" + SHA.hex() + "\",\"traceparent\":\"" + TRACE + "\","
                + "\"payload\":{\"payment_id\":\"pay_1\",\"order_id\":\"ord_1\",\"amount_paise\":49900}}");
        assertThat(EnvelopeCodec.decode(EnvelopeCodec.encode(e))).isEqualTo(e);
    }

    @Property(tries = 1_000)
    void everyEnvelopeRoundTripsAndReEncodesToTheSameBytes(@ForAll("envelopes") EventEnvelope e) {
        byte[] encoded = EnvelopeCodec.encode(e);
        EventEnvelope decoded = EnvelopeCodec.decode(encoded);
        assertThat(decoded).isEqualTo(e);
        assertThat(EnvelopeCodec.encode(decoded)).isEqualTo(encoded);
    }

    @Provide
    Arbitrary<EventEnvelope> envelopes() {
        Arbitrary<Long> paise = Arbitraries.longs().between(1, 10_000_000_000L);
        Arbitrary<String> id = Arbitraries.strings().withCharRange('a', 'z').numeric().withChars('_', '-').ofMinLength(1)
                .ofMaxLength(20);
        Arbitrary<Optional<String>> trace = Arbitraries.of(Optional.empty(), Optional.of(TRACE));
        Arbitrary<LocalDate> date = Arbitraries.integers().between(0, 3_000)
                .map(d -> LocalDate.of(2026, 1, 1).plusDays(d));
        Arbitrary<EventEnvelope> payment = Combinators.combine(Arbitraries.of(EventType.PAYMENT_PENDING,
                        EventType.PAYMENT_SUCCEEDED, EventType.PAYMENT_FAILED), id, id, paise, trace)
                .as((t, p, o, a, tr) -> envelope(Source.GATEWAY, t,
                        new PaymentPayload(PaymentId.of(p), OrderId.of(o), Money.ofPaise(a)), tr));
        Arbitrary<EventEnvelope> refund = Combinators.combine(Arbitraries.of(EventType.REFUND_PROCESSED,
                        EventType.REFUND_FAILED), id, paise, Arbitraries.of(Source.GATEWAY, Source.OBSERVATION))
                .as((t, p, a, s) -> envelope(s, t, new RefundPayload(PaymentId.of(p), Money.ofPaise(a)), Optional.empty()));
        Arbitrary<EventEnvelope> order = Combinators.combine(Arbitraries.of(EventType.ORDER_CREATED,
                        EventType.ORDER_PENDING_PAYMENT, EventType.ORDER_PAID, EventType.ORDER_FULFILLED,
                        EventType.ORDER_EXPIRED, EventType.ORDER_CANCELLED), id, Arbitraries.longs().between(1, 1_000),
                        paise, id)
                .as((t, o, v, a, p) -> envelope(Source.MERCHANT, t, new OrderPayload(OrderId.of(o), v, Money.ofPaise(a),
                        AT.plusSeconds(900), t.carriesPaidPayment() ? Optional.of(PaymentId.of(p)) : Optional.empty()),
                        Optional.empty()));
        Arbitrary<EventEnvelope> line = Combinators.combine(id, Arbitraries.integers().between(1, 10_000), date,
                        Arbitraries.longs().between(-1_000_000, 1_000_000), Arbitraries.longs().between(0, 5_000).optional(),
                        id.optional(), id.optional(), Arbitraries.strings().ofMaxLength(140))
                .as((f, n, d, a, fee, ref, batch, narration) -> envelope(Source.BANK, EventType.SETTLEMENT_LINE,
                        new SettlementLinePayload(f, n, d, d.minusDays(1), Money.ofPaise(a), fee.map(Money::ofPaise),
                                fee.map(x -> Money.ofPaise(x / 5)), ref, batch, narration), Optional.empty()));
        Arbitrary<EventEnvelope> end = Combinators.combine(id, date, Arbitraries.integers().between(0, 10_000), paise)
                .as((f, d, n, total) -> envelope(Source.BANK, EventType.SETTLEMENT_FILE_END,
                        new SettlementFileEndPayload(f, d, n, Money.ofPaise(total), SHA), Optional.empty()));
        return Arbitraries.oneOf(payment, refund, order, line, end);
    }

    @Test
    void anUnknownSchemaVersionIsRejected() {
        String body = utf8(EnvelopeCodec.encode(envelope(Source.GATEWAY, EventType.PAYMENT_FAILED,
                new PaymentPayload(PaymentId.of("p"), OrderId.of("o"), Money.ofPaise(1)), Optional.empty())))
                .replace("\"schema_version\":1", "\"schema_version\":2");
        assertThatThrownBy(() -> EnvelopeCodec.decode(body.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(MalformedJson.class).hasMessageContaining("schema_version");
    }

    @Test
    void anEnvelopeThatBreaksTheSchemaIsRejected() {
        // An order event claiming to come from the gateway: the envelope constructor refuses it.
        String body = utf8(EnvelopeCodec.encode(envelope(Source.MERCHANT, EventType.ORDER_CREATED,
                new OrderPayload(OrderId.of("o"), 1, Money.ofPaise(1), AT, Optional.empty()), Optional.empty())))
                .replace("\"source\":\"MERCHANT\"", "\"source\":\"GATEWAY\"");
        assertThatThrownBy(() -> EnvelopeCodec.decode(body.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(MalformedJson.class).hasMessageContaining("invalid envelope");
    }

    @Test
    void garbageAndUnknownEnumsAreRejectedWithoutEchoingValues() {
        assertThatThrownBy(() -> EnvelopeCodec.decode(new byte[] {(byte) 0xff, 0x00, 0x7b}))
                .isInstanceOf(MalformedJson.class);
        String body = utf8(EnvelopeCodec.encode(envelope(Source.GATEWAY, EventType.PAYMENT_FAILED,
                new PaymentPayload(PaymentId.of("p"), OrderId.of("o"), Money.ofPaise(1)), Optional.empty())));
        assertThatThrownBy(() -> EnvelopeCodec.decode(body.replace("GATEWAY", "secret-value")
                .getBytes(StandardCharsets.UTF_8))).isInstanceOf(MalformedJson.class)
                .hasMessage("source is not a known source");
        assertThatThrownBy(() -> EnvelopeCodec.decode(body.replace("payment.failed", "payment.exploded")
                .getBytes(StandardCharsets.UTF_8))).hasMessage("event_type is not a known event type");
    }
}
