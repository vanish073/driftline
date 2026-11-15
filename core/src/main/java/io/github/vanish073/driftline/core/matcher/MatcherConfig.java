package io.github.vanish073.driftline.core.matcher;

import java.time.Duration;
import java.util.Objects;

/**
 * The matcher's time constants (DESIGN §5.4, §5.5).
 *
 * @param boundGateway     bounded out-of-orderness of the gateway stream: {@code wm_G = max event time − bound}
 * @param boundMerchant    the same for the merchant stream
 * @param idleTimeout      a source with no event for this long (processing time) is idle, and its watermark follows
 *                         the clock: {@code max(wm, now − bound − skewAllowance)}
 * @param skewAllowance    how far a node clock may be off from the senders' clocks
 * @param orderGrace       how long after a success to wait for the order before DEBITED_NO_ORDER ({@code ORDER_CHECK})
 * @param graceDays        settlement days to wait past the due date before MISSING_SETTLEMENT ({@code SETTLEMENT_DUE})
 * @param expectedArrival  when on its settlement date (IST, from midnight) a merchant's file is expected
 * @param stallTimeout     how long after the expected arrival a missing file raises FEED_STALL
 */
public record MatcherConfig(Duration boundGateway, Duration boundMerchant, Duration idleTimeout, Duration skewAllowance,
                            Duration orderGrace, int graceDays, Duration expectedArrival, Duration stallTimeout) {

    /** The values DESIGN §5.5 names, plus bounds sized for webhook retry and clock-skew allowances. */
    public static final MatcherConfig DEFAULT = new MatcherConfig(Duration.ofMinutes(2), Duration.ofMinutes(2),
            Duration.ofMinutes(1), Duration.ofMinutes(2), Duration.ofMinutes(10), 1, Duration.ofHours(10),
            Duration.ofHours(12));

    public MatcherConfig {
        for (Duration d : new Duration[] {boundGateway, boundMerchant, idleTimeout, skewAllowance, orderGrace,
                expectedArrival, stallTimeout}) {
            Objects.requireNonNull(d, "duration");
            if (d.isNegative()) {
                throw new IllegalArgumentException("durations must not be negative");
            }
        }
        if (graceDays < 0) {
            throw new IllegalArgumentException("graceDays must not be negative");
        }
    }
}
