package io.github.vanish073.driftline.app.config;

import io.github.vanish073.driftline.store.postgres.BalanceStrategy;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Everything the app reads from configuration, prefix {@code driftline} (environment variables bind by Spring's relaxed
 * rules, for example {@code DRIFTLINE_KAFKA_BOOTSTRAP_SERVERS}).
 *
 * @param instanceId    this process's name in partition leases (§5.1); a restart appends a fresh incarnation
 * @param environment   {@code production} (the default), {@code dev} or {@code test} (Shift 11, D97): only dev and test
 *                      may apply a bootstrap file or take keys from the environment
 * @param bootstrapFile dev and test only: merchants, secrets and tokens to onboard at start-up (D49); {@code null} when
 *                      unset (Spring's binder has no converter to {@code Optional<Path>}, so read it through
 *                      {@link #bootstrap()})
 */
@ConfigurationProperties("driftline")
public record DriftlineProperties(
        @DefaultValue("local") String instanceId,
        @DefaultValue("production") String environment,
        @DefaultValue Kafka kafka,
        @DefaultValue Secrets secrets,
        @DefaultValue Pii pii,
        @DefaultValue Store store,
        @DefaultValue Worker worker,
        @DefaultValue Resolver resolver,
        @DefaultValue Dispatcher dispatcher,
        @DefaultValue Verify verify,
        @DefaultValue RateLimit rateLimit,
        @DefaultValue Metrics metrics,
        Path bootstrapFile) {

    /** The bootstrap file, if one is configured. */
    public Optional<Path> bootstrap() {
        return Optional.ofNullable(bootstrapFile);
    }

    /** The environments a deployment may name (D97). */
    public static final java.util.Set<String> ENVIRONMENTS = java.util.Set.of("production", "dev", "test");

    /** True for a dev or test stack (D97); anything else is treated as production. */
    public boolean devOrTest() {
        return environment.equals("dev") || environment.equals("test");
    }

    /**
     * §10.4. {@code partitions} must equal the topic's partition count; start-up checks it. Security (Shift 11, D100):
     * {@code securityProtocol} {@code SASL_SSL} logs in with {@code username} and {@code password} (SASL PLAIN, which
     * Kafka sends only inside TLS) and trusts the broker's certificate through {@code truststoreFile} (a PEM CA); a
     * production start refuses anything but {@code SASL_SSL}. {@code PLAINTEXT} is for tests' throwaway brokers.
     */
    public record Kafka(
            @DefaultValue("localhost:9092") String bootstrapServers,
            @DefaultValue("driftline.events.v1") String topic,
            @DefaultValue("12") int partitions,
            @DefaultValue("driftline-matcher") String groupId,
            @DefaultValue("100") int maxPollRecords,
            @DefaultValue("200ms") Duration pollTimeout,
            @DefaultValue("10s") Duration sessionTimeout,
            @DefaultValue("300s") Duration maxPollInterval,
            @DefaultValue("10s") Duration ackTimeout,
            @DefaultValue("60s") Duration maxTickInterval,
            @DefaultValue("PLAINTEXT") String securityProtocol,
            String username,
            String password,
            String truststoreFile) {

        /** Never prints the password. */
        @Override
        public String toString() {
            return "Kafka[bootstrapServers=" + bootstrapServers + ", topic=" + topic + ", partitions=" + partitions
                    + ", groupId=" + groupId + ", securityProtocol=" + securityProtocol + ", username=" + username
                    + ", password=" + (password == null || password.isBlank() ? "unset" : "***") + ", truststoreFile="
                    + truststoreFile + "]";
        }
    }

    /**
     * AES-256-GCM keys for stored webhook secrets, by key id, base64; {@code activeKid} encrypts (D48). Active secrets
     * are reloaded every {@code refreshInterval} (D76).
     */
    public record Secrets(@DefaultValue("") String activeKid, @DefaultValue Map<String, String> keys,
                          @DefaultValue("30s") Duration refreshInterval) {

        /** Never prints the keys. */
        @Override
        public String toString() {
            return "Secrets[activeKid=" + activeKid + ", keys=" + keys.keySet() + ", refreshInterval=" + refreshInterval
                    + "]";
        }
    }

    /**
     * Keys for customers' contact details (D77): an AES-256-GCM keyring (base64 by key id; {@code activeKid} encrypts)
     * separate from the webhook-secret keys, and a base64 HMAC key of at least 32 bytes for the VPA blind index. All
     * empty: personal data storage is off, and contacts on webhooks are discarded. Stored contacts are deleted once
     * first seen longer ago than {@code retention}, checked every {@code purgeInterval} by the resolver role (D99).
     */
    public record Pii(@DefaultValue("") String activeKid, @DefaultValue Map<String, String> keys,
                      @DefaultValue("") String indexKey, @DefaultValue("180d") Duration retention,
                      @DefaultValue("1h") Duration purgeInterval) {

        public boolean configured() {
            return !activeKid.isBlank() && !keys.isEmpty() && !indexKey.isBlank();
        }

        /** Never prints a key. */
        @Override
        public String toString() {
            return "Pii[activeKid=" + activeKid + ", keys=" + keys.keySet() + ", indexKey="
                    + (indexKey.isBlank() ? "unset" : "***") + ", retention=" + retention + ", purgeInterval="
                    + purgeInterval + "]";
        }
    }

    public record Store(@DefaultValue("APPEND_SNAPSHOT") BalanceStrategy balanceStrategy) {
    }

    /**
     * The management port's metrics endpoint (Shift 11, D98): Prometheus must send {@code Authorization: Bearer
     * <token>}. Unset, the endpoint refuses every scrape. Health stays open for the orchestrator's probes, and shows
     * only UP or DOWN.
     */
    public record Metrics(String token) {

        /** Never prints the token. */
        @Override
        public String toString() {
            return "Metrics[token=" + (token == null || token.isBlank() ? "unset" : "***") + "]";
        }
    }

    /**
     * Request limits ahead of the API (Shift 11, D96): a client address that gets {@code maxFailures} answers of 401
     * within {@code failureWindow} is refused for {@code lockout}; each credential (bearer token, webhook merchant,
     * else address) gets {@code requestsPerSecond} with bursts of {@code burst}. At most {@code maxTracked} addresses and
     * credentials are remembered. The defaults leave room for the load test's highest step, 400 events a second.
     */
    public record RateLimit(@DefaultValue("true") boolean enabled, @DefaultValue("20") int maxFailures,
                            @DefaultValue("60s") Duration failureWindow, @DefaultValue("5m") Duration lockout,
                            @DefaultValue("500") int requestsPerSecond, @DefaultValue("1000") int burst,
                            @DefaultValue("10000") int maxTracked) {
    }

    public record Worker(@DefaultValue("1s") Duration pollInterval, @DefaultValue("10") int batchSize) {
    }

    /**
     * The resolver role (§8): it sleeps at most {@code pollInterval}, since breaks are raised by another process. It also
     * reads the books for the metrics of §13 every {@code metricsInterval} (D78).
     */
    public record Resolver(@DefaultValue("1s") Duration pollInterval, @DefaultValue("20") int batchSize,
                           @DefaultValue("15s") Duration metricsInterval) {
    }

    /**
     * The dispatcher role (§9.2, D65). The target URLs and the service token (scope {@code observations:write}, sent only
     * to Driftline's own {@code observationsUrl}) have no defaults: the role refuses to start without them.
     */
    public record Dispatcher(
            URI gatewayUrl,
            URI merchantUrl,
            URI opsUrl,
            URI observationsUrl,
            String serviceToken,
            @DefaultValue("1s") Duration pollInterval,
            @DefaultValue("10s") Duration requestTimeout,
            @DefaultValue("30s") Duration callTimeout,
            @DefaultValue("5s") Duration backoffBase,
            @DefaultValue("10m") Duration backoffCap,
            @DefaultValue("20") int claimLimit) {

        /** @throws IllegalStateException naming every missing setting */
        public void requireComplete() {
            List<String> missing = new ArrayList<>();
            if (gatewayUrl == null) {
                missing.add("driftline.dispatcher.gateway-url");
            }
            if (merchantUrl == null) {
                missing.add("driftline.dispatcher.merchant-url");
            }
            if (opsUrl == null) {
                missing.add("driftline.dispatcher.ops-url");
            }
            if (observationsUrl == null) {
                missing.add("driftline.dispatcher.observations-url");
            }
            if (serviceToken == null || serviceToken.isBlank()) {
                missing.add("driftline.dispatcher.service-token");
            }
            if (!missing.isEmpty()) {
                throw new IllegalStateException("the dispatcher role needs " + String.join(", ", missing));
            }
            if (requestTimeout.compareTo(callTimeout) >= 0) {
                throw new IllegalStateException("driftline.dispatcher.request-timeout must be shorter than call-timeout, "
                        + "so a call is answered or abandoned before its lease runs out");
            }
        }

        /** Never prints the service token. */
        @Override
        public String toString() {
            return "Dispatcher[gatewayUrl=" + gatewayUrl + ", merchantUrl=" + merchantUrl + ", opsUrl=" + opsUrl
                    + ", observationsUrl=" + observationsUrl + ", serviceToken=" + (serviceToken == null ? "unset" : "***")
                    + ", pollInterval=" + pollInterval + ", callTimeout=" + callTimeout + "]";
        }
    }

    /**
     * {@code driftline verify} (role {@code verify}): the sandbox whose refund count is compared with the books (none:
     * the database checks only), and how long to keep re-checking before the verdict.
     */
    public record Verify(URI sandboxUrl, @DefaultValue("3m") Duration waitFor, @DefaultValue("5s") Duration every) {
    }
}
