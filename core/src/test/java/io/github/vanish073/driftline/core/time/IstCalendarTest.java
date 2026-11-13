package io.github.vanish073.driftline.core.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

class IstCalendarTest {

    private static final Instant LAST_MS_OF_13TH = OffsetDateTime.parse("2026-11-13T23:59:59.999+05:30").toInstant();
    private static final Instant FIRST_MS_OF_14TH = OffsetDateTime.parse("2026-11-14T00:00:00.000+05:30").toInstant();

    @Test
    void theIstDayChangesAtMidnightIstNotUtc() {
        assertThat(LAST_MS_OF_13TH).isEqualTo(Instant.parse("2026-11-13T18:29:59.999Z"));
        assertThat(FIRST_MS_OF_14TH).isEqualTo(Instant.parse("2026-11-13T18:30:00Z"));

        assertThat(IstCalendar.tDate(LAST_MS_OF_13TH)).isEqualTo(LocalDate.of(2026, 11, 13));
        assertThat(IstCalendar.tDate(FIRST_MS_OF_14TH)).isEqualTo(LocalDate.of(2026, 11, 14));
    }

    @Test
    void theEndOfAnIstDayIsTheNextMidnightExclusive() {
        assertThat(IstCalendar.startOfIstDay(LocalDate.of(2026, 11, 13))).isEqualTo(Instant.parse("2026-11-12T18:30:00Z"));
        assertThat(IstCalendar.endOfIstDay(LocalDate.of(2026, 11, 13))).isEqualTo(FIRST_MS_OF_14TH);
        assertThat(LAST_MS_OF_13TH).isBefore(IstCalendar.endOfIstDay(LocalDate.of(2026, 11, 13)));
    }

    @Test
    void tPlusFiveEndsAtMidnightIstStartingDaySix() {
        // Debited at 23:59:59.999 IST on the 13th: T = 13th, deadline = 00:00 IST on the 19th.
        assertThat(IstCalendar.deadlineAt(IstCalendar.tDate(LAST_MS_OF_13TH), 5))
                .isEqualTo(Instant.parse("2026-11-18T18:30:00Z"));
        // One millisecond later it is the 14th, so the deadline moves a whole day.
        assertThat(IstCalendar.deadlineAt(IstCalendar.tDate(FIRST_MS_OF_14TH), 5))
                .isEqualTo(Instant.parse("2026-11-19T18:30:00Z"));
    }

    @Test
    void deadlinesCrossMonthAndYearBoundaries() {
        assertThat(IstCalendar.deadlineAt(LocalDate.of(2026, 12, 29), 5)).isEqualTo(Instant.parse("2027-01-03T18:30:00Z"));
        assertThat(IstCalendar.deadlineAt(LocalDate.of(2028, 2, 28), 1)).isEqualTo(Instant.parse("2028-02-29T18:30:00Z"));
        assertThat(IstCalendar.deadlineAt(LocalDate.of(2026, 11, 13), 0)).isEqualTo(FIRST_MS_OF_14TH);
        assertThatThrownBy(() -> IstCalendar.deadlineAt(LocalDate.of(2026, 11, 13), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ignoresTheJvmDefaultTimeZone() {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Kiritimati"));
            assertThat(IstCalendar.tDate(LAST_MS_OF_13TH)).isEqualTo(LocalDate.of(2026, 11, 13));
            assertThat(IstCalendar.deadlineAt(LocalDate.of(2026, 11, 13), 5)).isEqualTo(Instant.parse("2026-11-18T18:30:00Z"));
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
