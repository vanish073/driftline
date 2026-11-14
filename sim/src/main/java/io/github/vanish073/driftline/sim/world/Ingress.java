package io.github.vanish073.driftline.sim.world;

import java.util.function.IntConsumer;

/** Where the world sends requests: Driftline's ingest. The reply carries the HTTP status. */
@FunctionalInterface
public interface Ingress {

    void receive(Request request, IntConsumer reply);
}
