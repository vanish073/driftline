package io.github.vanish073.driftline.wire.json;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * A parsed JSON object whose values are {@link String}, {@link Long}, {@link Boolean}, {@code null} or nested
 * {@code JsonObject}s. Accessors fail with {@link MalformedJson} naming the field, never the value.
 */
public final class JsonObject {

    private final String path;
    private final TreeMap<String, Object> fields;

    JsonObject(String path, TreeMap<String, Object> fields) {
        this.path = path;
        this.fields = fields;
    }

    public Map<String, Object> fields() {
        return Collections.unmodifiableMap(fields);
    }

    public boolean has(String name) {
        return fields.containsKey(name) && fields.get(name) != null;
    }

    public String string(String name) {
        return required(name, String.class, "a string");
    }

    public long integer(String name) {
        return required(name, Long.class, "an integer");
    }

    public JsonObject object(String name) {
        return required(name, JsonObject.class, "an object");
    }

    /** The string field, or null if it is absent or JSON null. */
    public String optionalString(String name) {
        return has(name) ? string(name) : null;
    }

    private <T> T required(String name, Class<T> type, String what) {
        Object value = fields.get(name);
        if (value == null) {
            throw new MalformedJson(path + name + " is required");
        }
        if (!type.isInstance(value)) {
            throw new MalformedJson(path + name + " must be " + what);
        }
        return type.cast(value);
    }
}
