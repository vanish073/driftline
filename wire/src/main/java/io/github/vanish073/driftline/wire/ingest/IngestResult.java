package io.github.vanish073.driftline.wire.ingest;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.wire.webhook.CustomerContact;
import java.util.Objects;
import java.util.Optional;

/** The pure result of normalising one request: an envelope to append to the log, or an HTTP rejection. */
public sealed interface IngestResult {

    /** {@code customer}: a webhook's contact details, kept out of the envelope (personal data, D77). */
    record Accepted(EventEnvelope envelope, Optional<CustomerContact> customer) implements IngestResult {
        public Accepted {
            Objects.requireNonNull(envelope, "envelope");
            Objects.requireNonNull(customer, "customer");
        }

        public Accepted(EventEnvelope envelope) {
            this(envelope, Optional.empty());
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
