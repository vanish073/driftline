package io.github.vanish073.driftline.wire;

import io.github.vanish073.driftline.wire.json.MalformedJson;
import java.time.DateTimeException;
import java.time.Instant;

/**
 * Instants on the wire: ISO-8601 in UTC with a {@code Z}, such as {@code 2026-11-13T06:00:00.123Z}. Precision finer
 * than a microsecond is rejected, because PostgreSQL stores microseconds and a stored state must equal the event it
 * came from.
 */
public final class WireTime {

    private WireTime() {
    }

    public static Instant parse(String field, String text) {
        if (!text.endsWith("Z")) {
            throw new MalformedJson(field + " must be an ISO-8601 UTC instant ending in Z");
        }
        Instant instant;
        try {
            instant = Instant.parse(text);
        } catch (DateTimeException e) {
            throw new MalformedJson(field + " is not an ISO-8601 instant");
        }
        if (instant.getNano() % 1_000 != 0) {
            throw new MalformedJson(field + " has sub-microsecond precision");
        }
        return instant;
    }

    public static String format(Instant instant) {
        return instant.toString();
    }
}
