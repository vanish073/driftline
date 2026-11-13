package io.github.vanish073.driftline.core.port;

import java.time.Instant;

/**
 * Processing time of this node (DESIGN §3.2). It may jump and may differ between nodes. It is never used as event
 * time. Production wires {@code SystemClock}; the simulator wires {@code SimClock}.
 */
public interface Clock {

    Instant now();
}
