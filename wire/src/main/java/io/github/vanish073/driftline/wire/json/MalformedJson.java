package io.github.vanish073.driftline.wire.json;

/** A body that is not valid under Driftline's strict JSON rules. Ingest answers 400. */
public final class MalformedJson extends IllegalArgumentException {

    public MalformedJson(String message) {
        super(message);
    }
}
