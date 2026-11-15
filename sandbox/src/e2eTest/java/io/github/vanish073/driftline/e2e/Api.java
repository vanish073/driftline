package io.github.vanish073.driftline.e2e;

import io.github.vanish073.driftline.sandbox.BootstrapFile;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

/** The running stack as the e2e suite sees it: Driftline's API, the sandbox, and the generated merchant pool. */
final class Api {

    static final URI DRIFTLINE = URI.create(env("DRIFTLINE_URL", "http://127.0.0.1:8080"));
    static final URI SANDBOX = URI.create(env("SANDBOX_URL", "http://127.0.0.1:8085"));
    static final BootstrapFile.Pool POOL = pool();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final JsonFactory JSON = new JsonFactory();

    private Api() {
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    private static BootstrapFile.Pool pool() {
        try {
            return BootstrapFile.read(Files.readAllBytes(Path.of(env("BOOTSTRAP_FILE",
                    "deploy/compose/.dev/bootstrap.json"))));
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the bootstrap file; run scripts/e2e.sh", e);
        }
    }

    static HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            return HTTP.send(request.timeout(Duration.ofMinutes(5)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static HttpRequest.Builder driftline(String path) {
        return HttpRequest.newBuilder(DRIFTLINE.resolve(path));
    }

    static HttpResponse<String> get(String path, String token) {
        return send(driftline(path).header("Authorization", "Bearer " + token).GET());
    }

    /**
     * Every break of {@code GET /v1/breaks} as a map of its scalar fields, plus {@code evidence_keys}: the names of its
     * evidence entries, sorted and comma-separated (the values are not needed).
     */
    static List<Map<String, String>> breaks(String json) {
        List<Map<String, String>> out = new ArrayList<>();
        try (JsonParser p = JSON.createParser(ObjectReadContext.empty(), json.getBytes(StandardCharsets.UTF_8))) {
            while (p.nextToken() != null) {
                if (p.currentToken() == JsonToken.PROPERTY_NAME && "breaks".equals(p.currentName())) {
                    p.nextToken();   // START_ARRAY
                    while (p.nextToken() == JsonToken.START_OBJECT) {
                        Map<String, String> row = new LinkedHashMap<>();
                        while (p.nextToken() == JsonToken.PROPERTY_NAME) {
                            String name = p.currentName();
                            JsonToken value = p.nextToken();
                            if (value == JsonToken.START_OBJECT && "evidence".equals(name)) {
                                TreeSet<String> keys = new TreeSet<>();
                                while (p.nextToken() == JsonToken.PROPERTY_NAME) {
                                    keys.add(p.currentName());
                                    JsonToken entry = p.nextToken();
                                    if (entry == JsonToken.START_OBJECT || entry == JsonToken.START_ARRAY) {
                                        p.skipChildren();
                                    }
                                }
                                row.put("evidence_keys", String.join(",", keys));
                            } else if (value == JsonToken.START_OBJECT || value == JsonToken.START_ARRAY) {
                                p.skipChildren();
                            } else {
                                row.put(name, p.getString());
                            }
                        }
                        out.add(row);
                    }
                }
            }
        }
        return out;
    }
}
