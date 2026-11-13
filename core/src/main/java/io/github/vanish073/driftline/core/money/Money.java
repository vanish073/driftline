package io.github.vanish073.driftline.core.money;

import java.util.Objects;

/**
 * An amount of Indian rupees, held as a {@code long} number of paise (1 rupee = 100 paise). DESIGN §4.1.
 *
 * <p>Every operation is exact. Arithmetic goes through {@link Math#addExact}, {@link Math#subtractExact} and
 * {@link Math#multiplyExact}, and throws {@link ArithmeticException} instead of wrapping around. The range is
 * symmetric, ±(2⁶³ − 1) paise: {@code Long.MIN_VALUE} is rejected, so {@link #negate()} can never overflow.
 */
public record Money(long paise) implements Comparable<Money> {

    public static final Money ZERO = new Money(0);

    public Money {
        if (paise == Long.MIN_VALUE) {
            throw new ArithmeticException("Money out of range: Long.MIN_VALUE paise has no positive counterpart");
        }
    }

    public static Money ofPaise(long paise) {
        return new Money(paise);
    }

    /**
     * Parses a non-negative rupee amount: ASCII digits, then optionally {@code '.'} and one or two digits.
     * {@code "1234.50"} gives 123450 paise and {@code "0.5"} gives 50.
     *
     * <p>Rejected: any sign, exponents ({@code "1e3"}), grouping ({@code "1,000"}), whitespace, leading zeros
     * ({@code "01"}), a bare point ({@code "1."}, {@code ".5"}), more than two decimal places, non-ASCII digits,
     * and values outside the range.
     *
     * @throws MoneyFormatException if the text is not a valid amount
     */
    public static Money parse(String text) {
        return parse(text, false);
    }

    /**
     * Like {@link #parse(String)}, but allows one leading {@code '-'}. A minus sign must mean a negative amount,
     * so {@code "-0"} and {@code "-0.00"} are rejected.
     *
     * @throws MoneyFormatException if the text is not a valid amount
     */
    public static Money parseSigned(String text) {
        return parse(text, true);
    }

    private static Money parse(String text, boolean signAllowed) {
        Objects.requireNonNull(text, "text");
        int length = text.length();
        int i = 0;
        boolean negative = false;
        if (signAllowed && length > 0 && text.charAt(0) == '-') {
            negative = true;
            i = 1;
        }

        int integerStart = i;
        while (i < length && isAsciiDigit(text.charAt(i))) {
            i++;
        }
        int integerEnd = i;
        if (integerEnd == integerStart) {
            throw new MoneyFormatException(text, "expected a digit at index " + integerStart);
        }
        if (integerEnd - integerStart > 1 && text.charAt(integerStart) == '0') {
            throw new MoneyFormatException(text, "leading zeros are not allowed");
        }

        int fractionStart = length;
        int fractionEnd = length;
        if (i < length) {
            if (text.charAt(i) != '.') {
                throw new MoneyFormatException(text, "unexpected character at index " + i);
            }
            fractionStart = ++i;
            while (i < length && isAsciiDigit(text.charAt(i))) {
                i++;
            }
            fractionEnd = i;
            if (fractionEnd == fractionStart) {
                throw new MoneyFormatException(text, "expected a digit after the decimal point");
            }
            if (fractionEnd - fractionStart > 2) {
                throw new MoneyFormatException(text, "more than 2 decimal places");
            }
            if (i < length) {
                throw new MoneyFormatException(text, "unexpected character at index " + i);
            }
        }

        long fractionPaise = 0;
        for (int f = fractionStart; f < fractionEnd; f++) {
            fractionPaise = fractionPaise * 10 + (text.charAt(f) - '0');
        }
        if (fractionEnd - fractionStart == 1) {
            fractionPaise *= 10;
        }

        long paise;
        try {
            long rupees = 0;
            for (int d = integerStart; d < integerEnd; d++) {
                rupees = Math.addExact(Math.multiplyExact(rupees, 10), text.charAt(d) - '0');
            }
            paise = Math.addExact(Math.multiplyExact(rupees, 100), fractionPaise);
        } catch (ArithmeticException overflow) {
            throw new MoneyFormatException(text, "outside the Money range");
        }

        if (negative) {
            if (paise == 0) {
                throw new MoneyFormatException(text, "negative zero is not allowed");
            }
            paise = -paise;
        }
        return new Money(paise);
    }

    private static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    public Money plus(Money other) {
        return new Money(Math.addExact(paise, other.paise));
    }

    public Money minus(Money other) {
        return new Money(Math.subtractExact(paise, other.paise));
    }

    public Money times(long factor) {
        return new Money(Math.multiplyExact(paise, factor));
    }

    public Money negate() {
        return new Money(Math.negateExact(paise));
    }

    public boolean isZero() {
        return paise == 0;
    }

    public boolean isPositive() {
        return paise > 0;
    }

    public boolean isNegative() {
        return paise < 0;
    }

    @Override
    public int compareTo(Money other) {
        return Long.compare(paise, other.paise);
    }

    /** The canonical rupee form with exactly two decimals, for example {@code "1234.50"} or {@code "-0.05"}. */
    @Override
    public String toString() {
        long magnitude = Math.absExact(paise);
        long rest = magnitude % 100;
        StringBuilder text = new StringBuilder(24);
        if (paise < 0) {
            text.append('-');
        }
        text.append(magnitude / 100).append('.');
        if (rest < 10) {
            text.append('0');
        }
        return text.append(rest).toString();
    }
}
