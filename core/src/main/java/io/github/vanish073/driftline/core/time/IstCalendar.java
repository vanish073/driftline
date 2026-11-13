package io.github.vanish073.driftline.core.time;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Business dates in India Standard Time (DESIGN §4.1, §8.3). Every conversion goes through the
 * {@code Asia/Kolkata} {@link ZoneId}, never a hard-coded offset, and never the JVM's default zone.
 */
public final class IstCalendar {

    public static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");

    private IstCalendar() {
    }

    /** The IST calendar date that {@code instant} falls on. */
    public static LocalDate istDate(Instant instant) {
        return LocalDate.ofInstant(instant, ZONE);
    }

    /** <b>T</b>: the IST calendar date of a payment's first debit evidence. */
    public static LocalDate tDate(Instant firstDebitEvidence) {
        return istDate(firstDebitEvidence);
    }

    /** 00:00 IST on {@code date}. */
    public static Instant startOfIstDay(LocalDate date) {
        return date.atStartOfDay(ZONE).toInstant();
    }

    /** The exclusive end of {@code date} in IST, which is 00:00 IST on the next day. */
    public static Instant endOfIstDay(LocalDate date) {
        return startOfIstDay(date.plusDays(1));
    }

    /**
     * The deadline instant of a T+{@code days} rule: the end of IST day T+days, which is 00:00 IST on day
     * T+days+1 (exclusive). For example T+5 from T = 2026-11-13 is 2026-11-19T00:00+05:30.
     */
    public static Instant deadlineAt(LocalDate tDate, int days) {
        if (days < 0) {
            throw new IllegalArgumentException("a deadline is T plus a non-negative number of days, not " + days);
        }
        return endOfIstDay(tDate.plusDays(days));
    }
}
