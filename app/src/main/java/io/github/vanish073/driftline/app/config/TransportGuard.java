package io.github.vanish073.driftline.app.config;

import io.github.vanish073.driftline.app.Roles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;

/**
 * A production start refuses plaintext connections (Shift 11, D100): Driftline's own links must use TLS, with the
 * other side's certificate checked. Dev and test stacks may run without (the integration tests' throwaway containers
 * do), though Compose and kind run with TLS throughout. Checked before anything connects:
 * <ul>
 *   <li>every role: the database URL asks for {@code sslmode=verify-full} (TLS, the CA and the host name checked);</li>
 *   <li>service roles: Kafka is {@code SASL_SSL}; the API port and the management port serve HTTPS (an SSL bundle);
 *       every dispatcher target is {@code https}; traces, if exported, go over {@code https}.</li>
 * </ul>
 */
public final class TransportGuard implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    private final String role;

    public TransportGuard(String role) {
        this.role = role;
    }

    /** What is not secured, given the role's settings; empty if everything is. */
    static List<String> problems(String role, Environment settings, Map<String, String> env) {
        List<String> problems = new ArrayList<>();
        String url = settings.getProperty("spring.datasource.url", "");
        if (!url.isEmpty() && !url.matches("jdbc:postgresql:.*[?&]sslmode=verify-full(&.*)?")) {
            problems.add("spring.datasource.url must ask for sslmode=verify-full");
        }
        if (Set.of(Roles.MIGRATE, Roles.VERIFY, Roles.VERIFY_AUDIT).contains(role)) {
            return problems;
        }
        if (!"SASL_SSL".equals(settings.getProperty("driftline.kafka.security-protocol", "PLAINTEXT"))) {
            problems.add("driftline.kafka.security-protocol must be SASL_SSL");
        }
        if (settings.getProperty("server.ssl.bundle", "").isBlank()) {
            problems.add("server.ssl.bundle must name an SSL bundle (the API serves HTTPS)");
        }
        if (settings.getProperty("management.server.ssl.bundle", "").isBlank()) {
            problems.add("management.server.ssl.bundle must name an SSL bundle (health and metrics serve HTTPS)");
        }
        for (String target : List.of("gateway-url", "merchant-url", "ops-url", "observations-url")) {
            String value = settings.getProperty("driftline.dispatcher." + target, "");
            if (!value.isEmpty() && !value.startsWith("https://")) {
                problems.add("driftline.dispatcher." + target + " must be https");
            }
        }
        if ("true".equalsIgnoreCase(env.getOrDefault("OTEL_JAVAAGENT_ENABLED", "false"))
                && !env.getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "").startsWith("https://")) {
            problems.add("OTEL_EXPORTER_OTLP_ENDPOINT must be https while tracing is on");
        }
        return problems;
    }

    /** @throws IllegalStateException naming every link not secured, unless the environment is dev or test */
    static void check(String role, Environment settings, Map<String, String> env) {
        String environment = settings.getProperty("driftline.environment", "production");
        if (environment.equals("dev") || environment.equals("test")) {
            return;
        }
        List<String> problems = problems(role, settings, env);
        if (!problems.isEmpty()) {
            throw new IllegalStateException("a production start needs TLS on every link (D100): "
                    + String.join("; ", problems));
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        check(role, event.getEnvironment(), System.getenv());
    }
}
