package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import java.util.Objects;

/** One record of the event log at {@code (partition, offset)}. Kafka in production, {@code SimLog} in the simulator. */
public record LogRecord(int partition, long offset, EventEnvelope envelope) {

    public LogRecord {
        Objects.requireNonNull(envelope, "envelope");
        if (partition < 0 || offset < 0) {
            throw new IllegalArgumentException("partition and offset must not be negative");
        }
        if (envelope.partition() != partition) {
            throw new IllegalArgumentException("an envelope for partition " + envelope.partition()
                    + " was read from partition " + partition);
        }
    }
}
