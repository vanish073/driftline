package io.github.vanish073.driftline.core.money;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class MoneyTest {

    @ParameterizedTest(name = "\"{0}\" is {1} paise")
    @CsvSource({
        "0, 0",
        "0.00, 0",
        "0.5, 50",
        "0.05, 5",
        "1, 100",
        "10, 1000",
        "1234.50, 123450",
        "1234.5, 123450",
        "92233720368547758.07, 9223372036854775807",
    })
    void parsesStrictRupeeAmounts(String text, long paise) {
        assertThat(Money.parse(text).paise()).isEqualTo(paise);
    }

    @ParameterizedTest(name = "\"{0}\" is rejected")
    @ValueSource(strings = {
        "",
        "1.234",                 // more than 2 decimal places
        "0.001",
        "-1",                    // no sign allowed
        "+1",
        "1e3",                   // no exponent
        "1E3",
        "1.5e2",
        "1,000.00",              // no grouping
        " 1",                    // no whitespace
        "1 ",
        "1.",                    // bare point
        ".5",
        "01",                    // no leading zeros
        "00.50",
        "1..2",
        "NaN",
        "Infinity",
        "0x10",
        "١",                // ARABIC-INDIC DIGIT ONE is not an ASCII digit
        "１",                // FULLWIDTH DIGIT ONE
        "92233720368547758.08",  // one paisa beyond Long.MAX_VALUE
        "100000000000000000",    // 10^17 rupees overflows
        "99999999999999999999",
    })
    void rejectsAnythingElse(String text) {
        assertThatThrownBy(() -> Money.parse(text)).isInstanceOf(MoneyFormatException.class);
    }

    @ParameterizedTest(name = "signed \"{0}\" is {1} paise")
    @CsvSource({
        "-1, -100",
        "-0.5, -50",
        "-0.05, -5",
        "12.34, 1234",
        "-92233720368547758.07, -9223372036854775807",
    })
    void parsesSignedAmounts(String text, long paise) {
        assertThat(Money.parseSigned(text).paise()).isEqualTo(paise);
    }

    @ParameterizedTest(name = "signed \"{0}\" is rejected")
    @ValueSource(strings = {"-0", "-0.0", "-0.00", "--1", "+1", "-", "-.5", "1-", "-92233720368547758.08", "-1e3"})
    void rejectsBadSignedAmounts(String text) {
        assertThatThrownBy(() -> Money.parseSigned(text)).isInstanceOf(MoneyFormatException.class);
    }

    @Test
    void rejectsNullText() {
        assertThatThrownBy(() -> Money.parse(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void arithmeticIsExact() {
        Money max = Money.ofPaise(Long.MAX_VALUE);
        assertThatThrownBy(() -> max.plus(Money.ofPaise(1))).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> max.negate().minus(Money.ofPaise(1))).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> max.times(2)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> Money.ofPaise(Long.MAX_VALUE / 2 + 1).times(2)).isInstanceOf(ArithmeticException.class);
        assertThat(Money.ofPaise(Long.MAX_VALUE / 2).times(2).paise()).isEqualTo(Long.MAX_VALUE - 1);
        assertThat(Money.ofPaise(150).plus(Money.ofPaise(-50))).isEqualTo(Money.ofPaise(100));
        assertThat(Money.ofPaise(150).minus(Money.ofPaise(200))).isEqualTo(Money.ofPaise(-50));
    }

    @Test
    void theRangeIsSymmetricSoNegateNeverOverflows() {
        assertThatThrownBy(() -> Money.ofPaise(Long.MIN_VALUE)).isInstanceOf(ArithmeticException.class);
        assertThat(Money.ofPaise(Long.MAX_VALUE).negate().paise()).isEqualTo(-Long.MAX_VALUE);
        assertThat(Money.ofPaise(-Long.MAX_VALUE).negate().paise()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void formatsWithExactlyTwoDecimals() {
        assertThat(Money.ofPaise(0)).hasToString("0.00");
        assertThat(Money.ofPaise(5)).hasToString("0.05");
        assertThat(Money.ofPaise(-5)).hasToString("-0.05");
        assertThat(Money.ofPaise(123450)).hasToString("1234.50");
        assertThat(Money.ofPaise(Long.MAX_VALUE)).hasToString("92233720368547758.07");
        assertThat(Money.ofPaise(-Long.MAX_VALUE)).hasToString("-92233720368547758.07");
    }

    @Test
    void ordersByAmount() {
        assertThat(Money.ofPaise(-1)).isLessThan(Money.ZERO);
        assertThat(Money.ofPaise(100)).isGreaterThan(Money.ofPaise(99));
        assertThat(Money.ZERO.isZero()).isTrue();
        assertThat(Money.ofPaise(1).isPositive()).isTrue();
        assertThat(Money.ofPaise(-1).isNegative()).isTrue();
    }

    @Property(tries = 2_000)
    void canonicalTextRoundTrips(@ForAll @LongRange(min = -Long.MAX_VALUE, max = Long.MAX_VALUE) long paise) {
        Money money = Money.ofPaise(paise);
        assertThat(Money.parseSigned(money.toString())).isEqualTo(money);
        if (paise >= 0) {
            assertThat(Money.parse(money.toString())).isEqualTo(money);
        }
    }
}
