package io.github.vanish073.driftline.core.money;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.RefundPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * D93: amounts from outside are bounded so the matcher's arithmetic cannot overflow. Before the limits, a settlement
 * line whose fee and tax were each near the 64-bit limit made {@code fee.plus(tax)} throw inside the matcher, which
 * stops the partition driver on every replay of the record.
 */
class AmountLimitsTest {

    static final Money ONE_PAISA = Money.ofPaise(1);
    static final LocalDate DAY = LocalDate.parse("2026-11-14");

    static SettlementLinePayload line(Money amount, Optional<Money> fee, Optional<Money> tax) {
        return new SettlementLinePayload("stl_1", 1, DAY, DAY, amount, fee, tax, Optional.empty(), Optional.empty(), "");
    }

    @Test
    void theLargestPaymentAtTheHighestRatesHasAFeeAndGstWithoutOverflow() {
        FeeSchedule highest = new FeeSchedule(10_000, 10_000);
        FeeSchedule.Breakdown b = highest.expected(AmountLimits.MAX_PAYMENT);
        assertThat(b.fee()).isEqualTo(AmountLimits.MAX_PAYMENT);
        assertThat(b.gst()).isEqualTo(AmountLimits.MAX_PAYMENT);
        // the old failure: the same arithmetic on an amount Money allows but the limits refuse
        assertThatThrownBy(() -> highest.expected(Money.ofPaise(Long.MAX_VALUE / 2)))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void theMatchersFeePlusTaxOnTheLargestSettlementFieldsCannotOverflow() {
        SettlementLinePayload l = line(AmountLimits.MAX_SETTLEMENT_FIELD, Optional.of(AmountLimits.MAX_SETTLEMENT_FIELD),
                Optional.of(AmountLimits.MAX_SETTLEMENT_FIELD));
        Money feePlusTax = l.fee().orElseThrow().plus(l.tax().orElseThrow());
        assertThat(l.amount().plus(feePlusTax).minus(feePlusTax.plus(AmountLimits.MAX_PAYMENT)))
                .isEqualTo(AmountLimits.MAX_SETTLEMENT_FIELD.minus(AmountLimits.MAX_PAYMENT));
        // a million lines at the limit still sum inside a long
        assertThat(Math.multiplyExact(AmountLimits.MAX_SETTLEMENT_FIELD.paise(), 1_000_000L / 100))
                .isPositive();
    }

    @Test
    void onePaisaAboveTheLimitIsRefusedEverywhereAnAmountEnters() {
        Money over = AmountLimits.MAX_PAYMENT.plus(ONE_PAISA);
        Instant t = Instant.parse("2026-11-14T10:00:00Z");
        assertThat(new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), AmountLimits.MAX_PAYMENT).amount())
                .isEqualTo(AmountLimits.MAX_PAYMENT);
        assertThatThrownBy(() -> new PaymentPayload(PaymentId.of("pay_1"), OrderId.of("ord_1"), over))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("largest accepted");
        assertThatThrownBy(() -> new RefundPayload(PaymentId.of("pay_1"), over))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("largest accepted");
        assertThatThrownBy(() -> new OrderPayload(OrderId.of("ord_1"), 1, over, t, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("largest accepted");

        Money overField = AmountLimits.MAX_SETTLEMENT_FIELD.plus(ONE_PAISA);
        assertThat(line(AmountLimits.MAX_SETTLEMENT_FIELD, Optional.empty(), Optional.empty()).amount())
                .isEqualTo(AmountLimits.MAX_SETTLEMENT_FIELD);
        assertThatThrownBy(() -> line(overField, Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("amount");
        assertThatThrownBy(() -> line(Money.ZERO, Optional.of(overField), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fee");
        assertThatThrownBy(() -> line(Money.ZERO, Optional.empty(), Optional.of(overField)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tax");
        assertThatThrownBy(() -> AmountLimits.requireSettlementField("entry BANK", overField.negate()))
                .as("an adjustment entry is signed: its size is what is bounded")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
