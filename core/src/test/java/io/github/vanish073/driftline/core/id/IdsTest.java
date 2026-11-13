package io.github.vanish073.driftline.core.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IdsTest {

    @ParameterizedTest
    @ValueSource(strings = {"pay_29QQoUBi66xm2f", "ORD-2026-0001", "m.001", "a", "0123456789012345678901234567890123456789012345678901234567890123"})
    void acceptsTokens(String value) {
        assertThat(PaymentId.of(value).value()).isEqualTo(value);
        assertThat(OrderId.of(value)).hasToString(value);
        assertThat(MerchantId.of(value).value()).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "pay 1", "pay/1", "pay:1", "pay\u00001", "pé", "01234567890123456789012345678901234567890123456789012345678901234"})
    void rejectsAnythingElse(String value) {
        assertThatThrownBy(() -> PaymentId.of(value)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void errorMessagesDoNotEchoTheValue() {
        assertThatThrownBy(() -> OrderId.of("secret value")).hasMessageNotContaining("secret");
    }

    @Test
    void keysArePrintableAscii() {
        assertThat(IdRules.requireKey("eventKey", "3f2a:17")).isEqualTo("3f2a:17");
        assertThatThrownBy(() -> IdRules.requireKey("eventKey", "a b")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> IdRules.requireKey("eventKey", "x".repeat(256))).isInstanceOf(IllegalArgumentException.class);
        assertThat(IdRules.requireKey("eventKey", "x".repeat(255))).hasSize(255);
    }

    @Test
    void idsSortByValue() {
        assertThat(PaymentId.of("pay_1")).isLessThan(PaymentId.of("pay_2"));
        assertThat(OrderId.of("A")).isLessThan(OrderId.of("a"));
    }
}
