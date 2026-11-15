package io.github.vanish073.driftline.sandbox;

import io.github.vanish073.driftline.core.time.IstCalendar;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;

/**
 * The sandbox CLI (DESIGN §15.2, D51).
 *
 * <pre>
 * sandbox bootstrap --out FILE [--merchants 12] [--partitions 12] [--days-back 3]
 * sandbox serve --bootstrap FILE --driftline http://driftline:8080 [--port 8085]
 * sandbox inject --type DUPLICATE_CAPTURE [--count 5] [--sandbox http://localhost:8085]
 * </pre>
 *
 * {@code bootstrap} writes a merchant pool with fresh random secrets and tokens whose first settlement date is
 * {@code days-back} days before today (IST), so every injected world lies in the past. {@code serve} runs the control
 * API; {@code inject} asks a running sandbox to play a scenario and prints its report.
 */
public final class SandboxMain {

    private SandboxMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0) {
            usage();
            return;
        }
        Map<String, String> opts = options(args);
        switch (args[0]) {
            case "bootstrap" -> {
                LocalDate first = IstCalendar.istDate(Instant.now())
                        .minusDays(Long.parseLong(opts.getOrDefault("days-back", "3")));
                byte[] file = BootstrapFile.generate(Integer.parseInt(opts.getOrDefault("merchants", "12")),
                        Integer.parseInt(opts.getOrDefault("partitions", "12")), first, new SecureRandom());
                Path out = Path.of(required(opts, "out"));
                Files.createDirectories(out.toAbsolutePath().getParent());
                Files.write(out, file);
                System.out.println("wrote " + out + " (first settlement date " + first + ")");
            }
            case "serve" -> {
                BootstrapFile.Pool pool = BootstrapFile.read(Files.readAllBytes(Path.of(required(opts, "bootstrap"))));
                SandboxServer server = new SandboxServer(Integer.parseInt(opts.getOrDefault("port", "8085")),
                        URI.create(required(opts, "driftline")), pool);
                server.start();
                System.out.println("sandbox listening on " + server.port() + " with " + pool.merchants().size()
                        + " merchants");
            }
            case "inject" -> {
                URI uri = URI.create(opts.getOrDefault("sandbox", "http://localhost:8085") + "/v1/inject?type="
                        + required(opts, "type") + "&count=" + opts.getOrDefault("count", "1"));
                HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofMinutes(10)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                        HttpResponse.BodyHandlers.ofString());
                System.out.print(response.body());
                if (response.statusCode() != 200) {
                    System.exit(1);
                }
            }
            default -> usage();
        }
    }

    private static Map<String, String> options(String[] args) {
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 1; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("expected --option value pairs, got " + args[i]);
            }
            out.put(args[i].substring(2), args[i + 1]);
        }
        return out;
    }

    private static String required(Map<String, String> opts, String name) {
        String v = opts.get(name);
        if (v == null) {
            throw new IllegalArgumentException("--" + name + " is required");
        }
        return v;
    }

    private static void usage() {
        System.out.println("""
                usage: sandbox bootstrap --out FILE [--merchants 12] [--partitions 12] [--days-back 3]
                       sandbox serve --bootstrap FILE --driftline URL [--port 8085]
                       sandbox inject --type SCENARIO [--count 1] [--sandbox URL]""");
    }
}
