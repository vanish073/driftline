package io.github.vanish073.driftline.core.payment;

import java.time.Instant;
import java.util.Optional;

/** Where an incoming event time falls relative to an instant the payment has recorded. */
public enum TimeOrder {
    /** The payment has no such instant recorded. */
    NO_RECORD,
    BEFORE,
    EQUAL,
    AFTER;

    public static TimeOrder of(Instant incoming, Optional<Instant> recorded) {
        if (recorded.isEmpty()) {
            return NO_RECORD;
        }
        int order = incoming.compareTo(recorded.get());
        return order < 0 ? BEFORE : order == 0 ? EQUAL : AFTER;
    }
}
