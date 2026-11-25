package io.github.vanish073.driftline.core.money;

/**
 * The largest amounts Driftline accepts from outside (Shift 11, D93). Without them an amount near the 64-bit limit,
 * which {@link Money} allows, could overflow later arithmetic: a payment's fee is its amount times up to 10,000 basis
 * points, and a short settlement line's fee and tax are added together. Overflow throws {@link ArithmeticException}
 * (never wraps), but in the matcher that would stop the partition driver on every replay of the record: one merchant's
 * file could halt reconciliation for all.
 *
 * <p>The limits are far above any real UPI amount and far below overflow:
 * <ul>
 *   <li>{@link #MAX_PAYMENT}, ₹10 crore, for a payment, a refund and an order. A fee is at most
 *       10<sup>10</sup> × 10<sup>4</sup> = 10<sup>14</sup> paise before rounding, its GST at most the same again, and a
 *       batch of even 10<sup>8</sup> matched payments sums to at most 10<sup>18</sup>, under the 9.2 × 10<sup>18</sup>
 *       limit of a {@code long}.</li>
 *   <li>{@link #MAX_SETTLEMENT_FIELD}, ₹1,000 crore, for each settlement line's amount, fee and tax (a batch line
 *       settles many payments), and for each entry of a manual ledger adjustment. A file's running total is already
 *       checked for overflow by its parser.</li>
 * </ul>
 */
public final class AmountLimits {

    /** ₹1,000 crore (10<sup>12</sup> paise): the largest payment, refund or order amount. */
    public static final Money MAX_PAYMENT = Money.ofPaise(1_000_000_000_000L);

    /** ₹1 lakh crore (10<sup>14</sup> paise): the largest settlement line amount, fee or tax, or adjustment entry. */
    public static final Money MAX_SETTLEMENT_FIELD = Money.ofPaise(100_000_000_000_000L);

    private AmountLimits() {
    }

    /** Throws if {@code amount} is above {@link #MAX_PAYMENT}; the message names the field, not the value. */
    public static Money requirePayment(String field, Money amount) {
        if (amount.compareTo(MAX_PAYMENT) > 0) {
            throw new IllegalArgumentException(field + " is above the largest accepted amount, " + MAX_PAYMENT);
        }
        return amount;
    }

    /** Throws if the size of {@code amount} is above {@link #MAX_SETTLEMENT_FIELD}. */
    public static Money requireSettlementField(String field, Money amount) {
        if (Math.abs(amount.paise()) > MAX_SETTLEMENT_FIELD.paise()) {   // Money excludes Long.MIN_VALUE
            throw new IllegalArgumentException(field + " is above the largest accepted amount, "
                    + MAX_SETTLEMENT_FIELD);
        }
        return amount;
    }
}
