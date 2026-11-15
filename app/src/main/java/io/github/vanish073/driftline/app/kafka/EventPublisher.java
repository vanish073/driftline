package io.github.vanish073.driftline.app.kafka;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import java.util.List;

/** Appends envelopes to the event log and returns once the log has acknowledged every one of them (§10.3). */
public interface EventPublisher {

    /** @throws EventLogUnavailable if any envelope was not acknowledged in time; some may still have been written */
    void publish(List<EventEnvelope> envelopes);

    /** The log did not acknowledge in time. The caller answers 503, and the sender retries with the same key. */
    final class EventLogUnavailable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public EventLogUnavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
