package io.github.vanish073.driftline.wire.ingest;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import java.util.Objects;

/** The pure result of normalising one request: an envelope to append to the log, or an HTTP rejection. */
public sealed interface IngestResult {

    record Accepted(EventEnvelope envelope) implements IngestResult {
        public Accepted {
            Objects.requireNonNull(envelope, "envelope");
        }
    }

    record Rejected(int status, String reason) implements IngestResult {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
            if (status < 400 || status > 499) {
                throw new IllegalArgumentException("a rejection is a 4xx status");
            }
        }
    }
}
