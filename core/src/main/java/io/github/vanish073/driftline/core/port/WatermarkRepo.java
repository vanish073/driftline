package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

/**
 * Stream watermarks per {@code (partition, source)} and the settlement watermark per merchant (DESIGN §5.4). Mirrors
 * {@code watermarks} and {@code settlement_watermarks} (§12). Neither ever moves back: a save that would lower a value
 * is refused with {@link IllegalArgumentException}, in both stores.
 */
public interface WatermarkRepo {

    /**
     * One stream watermark.
     *
     * @param value        the watermark: no event older than this is expected any more (a heuristic, §5.4)
     * @param maxEventTime the greatest event time seen from this source, if any
     * @param lastEventAt  the processing time at which the last event from this source was seen, if any
     */
    record StreamWatermark(int partition, Source source, Instant value, Optional<Instant> maxEventTime,
                           Optional<Instant> lastEventAt) {

        public StreamWatermark {
            if (partition < 0) {
                throw new IllegalArgumentException("partition must not be negative");
            }
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(value, "value");
            Objects.requireNonNull(maxEventTime, "maxEventTime");
            Objects.requireNonNull(lastEventAt, "lastEventAt");
            if (source != Source.GATEWAY && source != Source.MERCHANT) {
                throw new IllegalArgumentException("stream watermarks exist for GATEWAY and MERCHANT only");
            }
        }
    }

    Optional<StreamWatermark> stream(int partition, Source source);

    /**
     * Inserts or replaces the watermark of {@code (w.partition(), w.source())}.
     *
     * @throws IllegalArgumentException if {@code value} or {@code maxEventTime} would move back
     */
    void saveStream(StreamWatermark w);

    /** The settlement watermark {@code swm}: every expected file up to this date is COMPLETE. */
    Optional<LocalDate> settledThrough(MerchantId merchantId);

    /** @throws IllegalArgumentException if {@code date} is before the stored value */
    void saveSettledThrough(MerchantId merchantId, LocalDate date);
}
