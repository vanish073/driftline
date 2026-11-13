package io.github.vanish073.driftline.core.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DerivedIdTest {

    /** Expected values computed independently with Python's hashlib and base64 (2026-11-13). */
    @Test
    void matchesAnIndependentImplementation() {
        assertThat(DerivedId.of("refund", "m_001", "pay_001")).hasToString("ezw57ewwtx5bjbv7l4ve42nekd");
        assertThat(DerivedId.of("break", "m_001", "DUPLICATE_CAPTURE", "pay_002")).hasToString("h2hdq3c5n5pftx3sorallffex2");
        assertThat(DerivedId.of("ns")).hasToString("pp52nyacd4p5cgb5737gailpcm");
        assertThat(DerivedId.of("cmd", "ezw57ewwtx5bjbv7l4ve42nekd", "REFUND", "1")).hasToString("uxvzbqj25yc7k5sqryxfaxrmqi");
        assertThat(DerivedId.of("refund", "m_001", "pay_001", "é")).hasToString("77nbplajdu26cdot575texjtrs");
    }

    @Test
    void isStableAndSensitiveToEveryField() {
        DerivedId refund = DerivedId.of("refund", "m_001", "pay_001");
        assertThat(DerivedId.of("refund", "m_001", "pay_001")).isEqualTo(refund);
        assertThat(DerivedId.of("refund", "m_001", "pay_001x")).hasToString("uvfvoixibvl2gzx2je5c5p3fow");
        assertThat(DerivedId.of("refund", "m_002", "pay_001")).isNotEqualTo(refund);
        assertThat(DerivedId.of("refunds", "m_001", "pay_001")).isNotEqualTo(refund);
    }

    @Test
    void theSeparatorKeepsFieldBoundaries() {
        assertThat(DerivedId.of("ns", "ab", "c")).isNotEqualTo(DerivedId.of("ns", "a", "bc"));
        assertThat(DerivedId.of("ns", "a")).isNotEqualTo(DerivedId.of("ns", "a", "a"));
    }

    @Test
    void rejectsInputsThatWouldMakeTheEncodingAmbiguous() {
        assertThatThrownBy(() -> DerivedId.of("ns", "a\u0000b")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DerivedId.of("ns", "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DerivedId.of("Refund", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DerivedId.of("", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DerivedId.of("ns\u0000", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DerivedId.of("ns", (String) null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void validatesItsOwnShape() {
        assertThatThrownBy(() -> new DerivedId("short")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DerivedId("EZW57EWWTX5BJBV7L4VE42NEKD")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DerivedId("ezw57ewwtx5bjbv7l4ve42nek1")).isInstanceOf(IllegalArgumentException.class);
    }

    /** RFC 4648 §10 test vectors, lowercase and without padding. */
    @ParameterizedTest(name = "base32(\"{0}\") = \"{1}\"")
    @CsvSource(value = {"'', ''", "f, my", "fo, mzxq", "foo, mzxw6", "foob, mzxw6yq", "fooba, mzxw6ytb", "foobar, mzxw6ytboi"})
    void base32FollowsRfc4648(String input, String expected) {
        assertThat(Base32.encode(input.getBytes(StandardCharsets.US_ASCII))).isEqualTo(expected);
    }
}
