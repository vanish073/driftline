package io.github.vanish073.driftline.core.settlement;

import io.github.vanish073.driftline.core.money.Money;
import java.util.Objects;

/**
 * A merchant's contracted pricing (DESIGN §6.5, §12 {@code merchants.fee_bps, gst_bps}): a fee in basis points of the
 * gross, and GST in basis points of the fee, each rounded half up to the paisa. The <em>expected net</em> of a payment
 * is what a settlement line must pay for it to be MATCHED; anything below it is short (D16).
 */
public record FeeSchedule(long feeBps, long gstBps) {

    public FeeSchedule {
        if (feeBps < 0 || feeBps > 10_000 || gstBps < 0 || gstBps > 10_000) {
            throw new IllegalArgumentException("fee and GST rates must be 0 to 10000 basis points");
        }
    }

    /** What a settlement of {@code gross} is expected to look like. */
    public record Breakdown(Money gross, Money fee, Money gst, Money net) {

        public Breakdown {
            Objects.requireNonNull(gross, "gross");
            Objects.requireNonNull(fee, "fee");
            Objects.requireNonNull(gst, "gst");
            Objects.requireNonNull(net, "net");
            if (!net.plus(fee).plus(gst).equals(gross)) {
                throw new IllegalArgumentException("net + fee + gst must equal gross");
            }
        }
    }

    public Breakdown expected(Money gross) {
        if (!gross.isPositive()) {
            throw new IllegalArgumentException("gross must be positive");
        }
        long fee = roundHalfUp(Math.multiplyExact(gross.paise(), feeBps));
        long gst = roundHalfUp(Math.multiplyExact(fee, gstBps));
        long net = Math.subtractExact(Math.subtractExact(gross.paise(), fee), gst);
        return new Breakdown(gross, Money.ofPaise(fee), Money.ofPaise(gst), Money.ofPaise(net));
    }

    /** A basis-point product divided by 10,000, rounded half up (all inputs are non-negative). */
    private static long roundHalfUp(long basisPointProduct) {
        return Math.addExact(basisPointProduct, 5_000) / 10_000;
    }
}
