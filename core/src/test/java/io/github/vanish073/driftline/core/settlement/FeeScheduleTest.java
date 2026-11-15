package io.github.vanish073.driftline.core.settlement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.money.Money;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

/** The expected net of a payment (DESIGN §6.5, D16): fee and GST in basis points, rounded half up to the paisa. */
class FeeScheduleTest {

    private static final FeeSchedule TWO_PERCENT_PLUS_GST = new FeeSchedule(200, 1_800);

    @Test
    void aTypicalPaymentRoundsHalfUp() {
        FeeSchedule.Breakdown b = TWO_PERCENT_PLUS_GST.expected(Money.ofPaise(49_900));
        assertThat(b.fee()).isEqualTo(Money.ofPaise(998));
        assertThat(b.gst()).as("179.64 paise rounds to 180").isEqualTo(Money.ofPaise(180));
        assertThat(b.net()).isEqualTo(Money.ofPaise(48_722));
    }

    @Test
    void exactlyHalfAPaisaRoundsUp() {
        // 125 paise at 2% is 2.5 paise: rounds to 3. GST on 3 paise at 18% is 0.54: rounds to 1.
        FeeSchedule.Breakdown b = TWO_PERCENT_PLUS_GST.expected(Money.ofPaise(125));
        assertThat(b.fee()).isEqualTo(Money.ofPaise(3));
        assertThat(b.gst()).isEqualTo(Money.ofPaise(1));
        assertThat(b.net()).isEqualTo(Money.ofPaise(121));
        // 124 paise at 2% is 2.48 paise: rounds down to 2.
        assertThat(TWO_PERCENT_PLUS_GST.expected(Money.ofPaise(124)).fee()).isEqualTo(Money.ofPaise(2));
    }

    @Test
    void aZeroRateChargesNothing() {
        FeeSchedule.Breakdown b = new FeeSchedule(0, 1_800).expected(Money.ofPaise(10_000));
        assertThat(b.fee()).isEqualTo(Money.ZERO);
        assertThat(b.gst()).isEqualTo(Money.ZERO);
        assertThat(b.net()).isEqualTo(Money.ofPaise(10_000));
    }

    @Test
    void ratesAndAmountsOutsideTheirRangeAreRefused() {
        assertThatThrownBy(() -> new FeeSchedule(-1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FeeSchedule(0, 10_001)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TWO_PERCENT_PLUS_GST.expected(Money.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TWO_PERCENT_PLUS_GST.expected(Money.ofPaise(Long.MAX_VALUE)))
                .as("exact arithmetic, never a silent overflow").isInstanceOf(ArithmeticException.class);
    }

    @Property(tries = 2_000)
    void netPlusFeePlusGstIsAlwaysTheGross(@ForAll @LongRange(min = 1, max = 1_000_000_000_000L) long gross,
                                           @ForAll @LongRange(min = 0, max = 10_000) long feeBps,
                                           @ForAll @LongRange(min = 0, max = 10_000) long gstBps) {
        FeeSchedule.Breakdown b = new FeeSchedule(feeBps, gstBps).expected(Money.ofPaise(gross));
        assertThat(b.net().plus(b.fee()).plus(b.gst())).isEqualTo(Money.ofPaise(gross));
        assertThat(b.fee().isNegative() || b.gst().isNegative()).isFalse();
    }
}
