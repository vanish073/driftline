package io.github.vanish073.driftline.core.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class EventEnvelopeTest {

    private static final Instant AT = Instant.parse("2026-11-13T06:00:00Z");
    private static final Sha256 HASH = Sha256.of("payload".getBytes(StandardCharsets.UTF_8));
    private static final PaymentPayload PAYMENT = new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), Money.ofPaise(49_900));
    private static final String TRACE = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private static EventEnvelope envelope(Source source, EventType type, EventPayload payload, Optional<String> traceparent) {
        return new EventEnvelope(1, MerchantId.of("m_001"), 3, source, "evt_1", type, AT, AT.plusSeconds(1), HASH,
                payload, traceparent);
    }

    private static OrderPayload order(Optional<PaymentId> paidWith) {
        return new OrderPayload(OrderId.of("ord_1"), 2, Money.ofPaise(49_900), AT.plusSeconds(900), paidWith);
    }

    @Test
    void acceptsAWellFormedEvent() {
        EventEnvelope e = envelope(Source.GATEWAY, EventType.PAYMENT_SUCCEEDED, PAYMENT, Optional.of(TRACE));
        assertThat(e.payload()).isEqualTo(PAYMENT);
        assertThat(envelope(Source.OBSERVATION, EventType.PAYMENT_SUCCEEDED, PAYMENT, Optional.empty()).source())
                .isEqualTo(Source.OBSERVATION);
    }

    @Test
    void sha256MatchesTheStandardVector() {
        assertThat(Sha256.of("abc".getBytes(StandardCharsets.US_ASCII)))
                .hasToString("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assertThatThrownBy(() -> new Sha256("BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsASourceTheEventTypeDoesNotAllow() {
        assertThatThrownBy(() -> envelope(Source.MERCHANT, EventType.PAYMENT_SUCCEEDED, PAYMENT, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot come from");
        assertThatThrownBy(() -> envelope(Source.OBSERVATION, EventType.ORDER_PAID, order(Optional.of(PaymentId.of("pay_1"))), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAPayloadOfTheWrongType() {
        assertThatThrownBy(() -> envelope(Source.MERCHANT, EventType.ORDER_CREATED, PAYMENT, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("carries a OrderPayload");
    }

    @Test
    void orderSnapshotsNameThePaymentExactlyWhenPaidOrFulfilled() {
        Optional<PaymentId> pay = Optional.of(PaymentId.of("pay_1"));
        assertThat(envelope(Source.MERCHANT, EventType.ORDER_PAID, order(pay), Optional.empty())).isNotNull();
        assertThat(envelope(Source.MERCHANT, EventType.ORDER_FULFILLED, order(pay), Optional.empty())).isNotNull();
        assertThatThrownBy(() -> envelope(Source.MERCHANT, EventType.ORDER_PAID, order(Optional.empty()), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> envelope(Source.MERCHANT, EventType.ORDER_EXPIRED, order(pay), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",       // no flags
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",    // unknown version
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",    // uppercase
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",    // zero trace id
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",    // zero parent id
        "00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01",
    })
    void rejectsAMalformedTraceparent(String traceparent) {
        assertThatThrownBy(() -> envelope(Source.GATEWAY, EventType.PAYMENT_PENDING, PAYMENT, Optional.of(traceparent)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOtherSchemaVersionsAndBadKeys() {
        assertThatThrownBy(() -> new EventEnvelope(2, MerchantId.of("m_001"), 0, Source.GATEWAY, "evt_1",
                EventType.PAYMENT_PENDING, AT, AT, HASH, PAYMENT, Optional.empty())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EventEnvelope(1, MerchantId.of("m_001"), 0, Source.GATEWAY, "evt 1",
                EventType.PAYMENT_PENDING, AT, AT, HASH, PAYMENT, Optional.empty())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EventEnvelope(1, MerchantId.of("m_001"), -1, Source.GATEWAY, "evt_1",
                EventType.PAYMENT_PENDING, AT, AT, HASH, PAYMENT, Optional.empty())).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(EventType.class)
    void wireNamesRoundTrip(EventType type) {
        assertThat(EventType.fromWireName(type.wireName())).isEqualTo(type);
    }

    @Test
    void settlementPayloadsCheckTheirStructure() {
        LocalDate day = LocalDate.of(2026, 11, 14);
        assertThat(new SettlementLinePayload("F-1", 1, day, day, Money.ofPaise(100), Optional.of(Money.ofPaise(2)),
                Optional.empty(), Optional.of("pay_1"), Optional.empty(), "UPI settlement")).isNotNull();
        assertThatThrownBy(() -> new SettlementLinePayload("F-1", 0, day, day, Money.ofPaise(100), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SettlementLinePayload("F-1", 1, day, day, Money.ofPaise(100), Optional.of(Money.ofPaise(-2)),
                Optional.empty(), Optional.empty(), Optional.empty(), "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SettlementFileEndPayload("F-1", day, -1, Money.ZERO, HASH))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
