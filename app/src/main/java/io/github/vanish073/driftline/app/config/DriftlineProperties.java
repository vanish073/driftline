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
 * @param bootstrapFile dev and test only: merchants, secrets and tokens to onboard at start-up (D49); {@code null} when
 *                      unset (Spring's binder has no converter to {@code Optional<Path>}, so read it through
 *                      {@link #bootstrap()})
 */
@ConfigurationProperties("driftline")
public record DriftlineProperties(
        @DefaultValue("local") String instanceId,
        @DefaultValue Kafka kafka,
        @DefaultValue Secrets secrets,
        @DefaultValue Store store,
        @DefaultValue Worker worker,
        @DefaultValue Resolver resolver,
        @DefaultValue Dispatcher dispatcher,
        @DefaultValue Verify verify,
        Path bootstrapFile) {

    /** The bootstrap file, if one is configured. */
    public Optional<Path> bootstrap() {
        return Optional.ofNullable(bootstrapFile);
    }

    /** §10.4. {@code partitions} must equal the topic's partition count; start-up checks it. */
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
            @DefaultValue("60s") Duration maxTickInterval) {
    }

    /** AES-256-GCM keys for stored webhook secrets, by key id, base64; {@code activeKid} encrypts (D48). */
    public record Secrets(@DefaultValue("") String activeKid, @DefaultValue Map<String, String> keys) {
    }

    public record Store(@DefaultValue("APPEND_SNAPSHOT") BalanceStrategy balanceStrategy) {
    }

    public record Worker(@DefaultValue("1s") Duration pollInterval, @DefaultValue("10") int batchSize) {
    }

    /** The resolver role (§8): it sleeps at most {@code pollInterval}, since breaks are raised by another process. */
    public record Resolver(@DefaultValue("1s") Duration pollInterval, @DefaultValue("20") int batchSize) {
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
