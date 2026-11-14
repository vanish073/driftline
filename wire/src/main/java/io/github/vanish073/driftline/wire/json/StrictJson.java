package io.github.vanish073.driftline.wire.json;

import java.io.ByteArrayOutputStream;
import java.util.TreeMap;
import java.util.function.Consumer;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;

/**
 * Strict JSON over Jackson's streaming core. Reading rejects duplicate keys (two parsers could otherwise see two
 * different bodies under one signature), fractional and exponent numbers (amounts are integer paise), arrays, nesting
 * deeper than {@value #MAX_DEPTH}, and trailing content. Writing emits fields in call order with no whitespace, which
 * is what makes an encoding canonical.
 */
public final class StrictJson {

    public static final int MAX_DEPTH = 3;

    private static final JsonFactory FACTORY = JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build();

    private StrictJson() {
    }

    public static JsonObject parse(byte[] body) {
        try (JsonParser parser = FACTORY.createParser(ObjectReadContext.empty(), body)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new MalformedJson("the body must be a JSON object");
            }
            JsonObject root = readObject(parser, "", 1);
            if (parser.nextToken() != null) {
                throw new MalformedJson("unexpected content after the JSON object");
            }
            return root;
        } catch (JacksonException e) {
            throw new MalformedJson("not valid JSON: " + e.getOriginalMessage());
        }
    }

    private static JsonObject readObject(JsonParser parser, String path, int depth) {
        if (depth > MAX_DEPTH) {
            throw new MalformedJson(path + " is nested too deeply");
        }
        TreeMap<String, Object> fields = new TreeMap<>();
        while (true) {
            JsonToken token = parser.nextToken();
            if (token == JsonToken.END_OBJECT) {
                return new JsonObject(path, fields);
            }
            String name = parser.currentName();
            JsonToken value = parser.nextToken();
            Object parsed = switch (value) {
                case VALUE_STRING -> parser.getString();
                case VALUE_NUMBER_INT -> {
                    if (parser.getNumberType() != JsonParser.NumberType.INT && parser.getNumberType() != JsonParser.NumberType.LONG) {
                        throw new MalformedJson(path + name + " does not fit a 64-bit integer");
                    }
                    yield parser.getLongValue();
                }
                case VALUE_TRUE -> Boolean.TRUE;
                case VALUE_FALSE -> Boolean.FALSE;
                case VALUE_NULL -> null;
                case START_OBJECT -> readObject(parser, path + name + ".", depth + 1);
                case VALUE_NUMBER_FLOAT -> throw new MalformedJson(path + name + " must be an integer, not a fraction");
                default -> throw new MalformedJson(path + name + " has an unsupported JSON type");
            };
            fields.put(name, parsed);
        }
    }

    /** Writes one object with the generator in call order and returns the UTF-8 bytes. */
    public static byte[] write(Consumer<JsonGenerator> body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(256);
        try (JsonGenerator generator = FACTORY.createGenerator(ObjectWriteContext.empty(), out)) {
            generator.writeStartObject();
            body.accept(generator);
            generator.writeEndObject();
        }
        return out.toByteArray();
    }
}
