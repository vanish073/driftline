package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Timers (DESIGN §5.5), unique per {@code (merchant, kind, subjectKey)}: registering one again moves it (an upsert).
 * Mirrors {@code timers} (§12). Due timers are listed in the deterministic firing order {@code (fireAt, kind,
 * subjectKey)}, then merchant, in both stores.
 */
public interface TimerRepo {

    /** The timers of §5.5 that exist so far; {@code PENDING_PROBE} and {@code BATCH_SOLVE} arrive in Shifts 8 and 7. */
    enum Kind {
        ORDER_CHECK,
        DUP_CHECK,
        PENDING_PROBE,
        SETTLEMENT_DUE,
        BATCH_SOLVE,
        FEED_STALL
    }

    /** Which time a timer waits for (§5.5). */
    enum TimerClock {
        /** {@code min(wm_G, wm_M)} of the partition. */
        STREAM_GM,
        /** {@code wm_G} of the partition. */
        STREAM_G,
        /** The merchant's settlement watermark, as the start of the IST day after {@code swm}. */
        SETTLEMENT,
        /** The matcher's processing clock. */
        PROCESSING
    }

    record Timer(MerchantId merchantId, Kind kind, String subjectKey, int partition, TimerClock clock, Instant fireAt) {

        public Timer {
            Objects.requireNonNull(merchantId, "merchantId");
            Objects.requireNonNull(kind, "kind");
            IdRules.requireKey("subjectKey", subjectKey);
            Objects.requireNonNull(clock, "clock");
            Objects.requireNonNull(fireAt, "fireAt");
            if (partition < 0) {
                throw new IllegalArgumentException("partition must not be negative");
            }
        }
    }

    /** Inserts the timer, or moves the existing one with the same {@code (merchant, kind, subjectKey)}. */
    void upsert(Timer timer);

    Optional<Timer> find(MerchantId merchantId, Kind kind, String subjectKey);

    /** The partition's timers on {@code clock} with {@code fireAt <= upTo}, in firing order. */
    List<Timer> due(int partition, TimerClock clock, Instant upTo);

    /** The merchant's timers on {@code clock} with {@code fireAt <= upTo}, in firing order. */
    List<Timer> dueForMerchant(MerchantId merchantId, TimerClock clock, Instant upTo);

    /** The earliest {@code fireAt} of the partition's timers on {@code clock}, if any. */
    Optional<Instant> earliest(int partition, TimerClock clock);

    /** Removes the timer if it exists (a fired timer is deleted in the transaction that fired it). */
    void delete(MerchantId merchantId, Kind kind, String subjectKey);
}
