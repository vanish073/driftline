package io.github.vanish073.driftline.app.config;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;

/**
 * Keys and passwords come from files, not environment variables (Shift 11, D97). Environment variables leak: every
 * child process inherits them, {@code docker inspect} and the Kubernetes API show them, and crash reports and
 * {@code /proc/<pid>/environ} include them. Driftline reads secrets from a directory of files, one per setting
 * ({@code spring.config.import=optional:configtree:${DRIFTLINE_SECRETS_DIR:/run/secrets/driftline/}}), which Compose
 * and Kubernetes mount read-only from their secret stores. A start with {@code driftline.environment} other than dev or
 * test refuses to continue if any secret setting is in the process environment, naming the variables (never values).
 */
public final class SecretSourceGuard implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    /** Settings that are secrets, as environment variable names: exact names and prefixes (a keyring's kids). */
    static final List<String> SECRET_NAMES = List.of("SPRING_DATASOURCE_PASSWORD", "DRIFTLINE_PII_INDEX_KEY",
            "DRIFTLINE_DISPATCHER_SERVICE_TOKEN", "DRIFTLINE_KAFKA_PASSWORD", "DRIFTLINE_METRICS_TOKEN");
    static final List<String> SECRET_PREFIXES = List.of("DRIFTLINE_SECRETS_KEYS_", "DRIFTLINE_PII_KEYS_");

    /** The secret settings found in {@code env}, sorted; empty if none. */
    static List<String> secretsIn(Map<String, String> env) {
        TreeSet<String> found = new TreeSet<>();
        for (String name : env.keySet()) {
            String upper = name.toUpperCase(java.util.Locale.ROOT);
            if (SECRET_NAMES.contains(upper) || SECRET_PREFIXES.stream().anyMatch(upper::startsWith)) {
                found.add(name);
            }
        }
        return List.copyOf(found);
    }

    /** @throws IllegalStateException if {@code environment} is not dev or test and {@code env} holds a secret */
    static void check(String environment, Map<String, String> env) {
        if (environment.equals("dev") || environment.equals("test")) {
            return;
        }
        List<String> found = secretsIn(env);
        if (!found.isEmpty()) {
            throw new IllegalStateException("secrets must be files under the secrets directory (DRIFTLINE_SECRETS_DIR, "
                    + "default /run/secrets/driftline/, one file per setting), not environment variables, unless "
                    + "driftline.environment is dev or test; found in the environment: " + String.join(", ", found));
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        check(event.getEnvironment().getProperty("driftline.environment", "production"), System.getenv());
    }
}
