package io.github.vanish073.driftline.app.config;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.dispatcher.CallCodecs;
import io.github.vanish073.driftline.app.dispatcher.DispatcherDriver;
import io.github.vanish073.driftline.app.kafka.EventPublisher;
import io.github.vanish073.driftline.app.kafka.KafkaClients;
import io.github.vanish073.driftline.app.kafka.KafkaEventPublisher;
import io.github.vanish073.driftline.app.kafka.PartitionDriver;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.port.HttpNetwork;
import io.github.vanish073.driftline.app.resolver.ResolverDriver;
import io.github.vanish073.driftline.app.settlement.SettlementWorker;
import io.github.vanish073.driftline.core.dispatcher.Dispatcher;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MatcherConfig;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * What each role runs (DESIGN §3.3, D49): the publisher, the partition driver, the settlement worker, the resolver and
 * the dispatcher.
 */
@Configuration
@Profile(Roles.SERVICE)
public class RoleConfiguration {

    /** Ingest and the settlement worker write to Kafka. */
    @Bean(destroyMethod = "close")
    @Profile({Roles.INGEST, Roles.SETTLEMENT_WORKER})
    KafkaEventPublisher eventPublisher(DriftlineProperties properties, MerchantRegistry merchants) {
        DriftlineProperties.Kafka kafka = properties.kafka();
        KafkaEventPublisher publisher = new KafkaEventPublisher(
                new KafkaProducer<>(KafkaClients.producer(kafka, "driftline-producer-" + leaseOwner(properties))),
                kafka.topic(), kafka.ackTimeout());
        publisher.verifyTopic(kafka.partitions(), merchants.maxPartition());
        return publisher;
    }

    @Bean
    @Profile(Roles.MATCHER)
    Matcher matcher(MerchantRegistry merchants, Canaries canaries) {
        return new Matcher(merchants.directory(), MatcherConfig.DEFAULT, canaries);
    }

    @Bean
    @Profile(Roles.MATCHER)
    PartitionDriver partitionDriver(Store store, Matcher matcher, Clock clock, CrashPoints crashPoints,
                                    DriftlineProperties properties) {
        DriftlineProperties.Kafka kafka = properties.kafka();
        String owner = leaseOwner(properties);
        return new PartitionDriver(store, matcher, clock,
                () -> new KafkaConsumer<>(KafkaClients.consumer(kafka, "driftline-matcher-" + owner)), kafka.topic(),
                owner, crashPoints, PartitionDriver.Listener.NONE, kafka.pollTimeout(), kafka.maxTickInterval(),
                kafka.sessionTimeout().multipliedBy(3));
    }

    @Bean
    @Profile(Roles.MATCHER)
    HealthIndicator partitionDriverHealth(PartitionDriver driver) {
        return () -> driver.failure()
                .map(f -> Health.down().withDetail("stopped", f.getClass().getSimpleName()).build())
                .orElseGet(() -> Health.up().withDetail("partitions", driver.ownedPartitions()).build());
    }

    @Bean
    @Profile(Roles.SETTLEMENT_WORKER)
    SettlementWorker settlementWorker(SettlementInbox inbox, Store store, EventPublisher publisher,
                                      MerchantRegistry merchants, Clock clock, CrashPoints crashPoints,
                                      DriftlineProperties properties) {
        return new SettlementWorker(inbox, store, publisher, merchants, clock, crashPoints,
                properties.worker().pollInterval(), properties.worker().batchSize());
    }

    @Bean
    @Profile(Roles.SETTLEMENT_WORKER)
    HealthIndicator settlementWorkerHealth(SettlementWorker worker) {
        return () -> worker.failure()
                .map(f -> Health.down().withDetail("stopped", f.getClass().getSimpleName()).build())
                .orElseGet(() -> Health.up().build());
    }

    /**
     * The resolver (§8). In a JVM that also runs the dispatcher, a step that decided something wakes it at once;
     * otherwise the dispatcher finds the commands within its poll interval.
     */
    @Bean
    @Profile(Roles.RESOLVER)
    ResolverDriver resolverDriver(Store store, Clock clock, Canaries canaries, CrashPoints crashPoints,
                                  DriftlineProperties properties, ObjectProvider<DispatcherDriver> dispatcher) {
        DriftlineProperties.Resolver r = properties.resolver();
        return new ResolverDriver(store, new Resolver(Resolver.Config.DEFAULT, canaries, crashPoints), clock,
                crashPoints, r.pollInterval(), r.batchSize(), () -> dispatcher.ifAvailable(DispatcherDriver::wake));
    }

    @Bean
    @Profile(Roles.RESOLVER)
    HealthIndicator resolverHealth(ResolverDriver driver) {
        return () -> driver.failure()
                .map(f -> Health.down().withDetail("stopped", f.getClass().getSimpleName()).build())
                .orElseGet(() -> Health.up().build());
    }

    /** The dispatcher (§9.2, D65): refuses to start without its target URLs and service token. */
    @Bean
    @Profile(Roles.DISPATCHER)
    DispatcherDriver dispatcherDriver(Store store, Clock clock, Canaries canaries, CrashPoints crashPoints,
                                      DriftlineProperties properties) {
        DriftlineProperties.Dispatcher d = properties.dispatcher();
        d.requireComplete();
        Dispatcher dispatcher = new Dispatcher(new Dispatcher.Config(d.callTimeout(), d.backoffBase(), d.backoffCap(),
                d.gatewayKeyTtl(), d.lookupMargin(), d.claimLimit()), canaries, crashPoints);
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        Map<CallTarget, URI> targets = Map.of(CallTarget.GATEWAY, d.gatewayUrl(), CallTarget.MERCHANT,
                d.merchantUrl(), CallTarget.OPS, d.opsUrl(), CallTarget.DRIFTLINE, d.observationsUrl());
        return new DispatcherDriver(store, dispatcher, clock, crashPoints,
                inbox -> new HttpNetwork(client, targets, CallCodecs.all(d.serviceToken()), inbox, d.requestTimeout()),
                leaseOwner(properties), d.pollInterval());
    }

    @Bean
    @Profile(Roles.DISPATCHER)
    HealthIndicator dispatcherHealth(DispatcherDriver driver) {
        return () -> driver.failure()
                .map(f -> Health.down().withDetail("stopped", f.getClass().getSimpleName()).build())
                .orElseGet(() -> Health.up().build());
    }

    /**
     * This process's name in partition leases: the configured instance id plus its start second, so a restarted
     * process is a new owner, as a new Kafka member is (§5.1). Restricted to the id alphabet.
     */
    static String leaseOwner(DriftlineProperties properties) {
        String base = properties.instanceId().replaceAll("[^A-Za-z0-9._-]", "-");
        String owner = base + "." + STARTED;
        return owner.length() <= 64 ? owner : owner.substring(owner.length() - 64);
    }

    private static final long STARTED = Instant.now().getEpochSecond();
}
