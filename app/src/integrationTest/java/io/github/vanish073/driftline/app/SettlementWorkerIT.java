package io.github.vanish073.driftline.app;

import static io.github.vanish073.driftline.app.Containers.await;
import static io.github.vanish073.driftline.app.Containers.count;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.app.config.DriftlineProperties;
import io.github.vanish073.driftline.app.kafka.KafkaClients;
import io.github.vanish073.driftline.app.kafka.KafkaEventPublisher;
import io.github.vanish073.driftline.app.kafka.PartitionDriver;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.app.port.SystemClock;
import io.github.vanish073.driftline.app.settlement.SettlementWorker;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.BalanceStrategy;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.PostgresStore;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

/**
 * The separate settlement worker against real PostgreSQL and Kafka (DESIGN §6.2, §9.4 CP8): a crash after the records
 * were emitted but before the upload was marked done leaves it RECEIVED; the next pass emits everything again, and the
 * matcher's dedupe makes the file COMPLETE once, with each line stored once.
 */
class SettlementWorkerIT {

    static final MerchantId M = MerchantId.of("m_worker");
    static final String FILE = """
            H,DLSF,1,stl_worker_1,m_worker,2026-11-14,2026-11-14T04:30:00Z
            D,1,2026-11-14,100.00,,,pay_unknown_1,UTR1,first
            D,2,2026-11-14,200.00,,,pay_unknown_2,UTR1,second
            T,2,300.00
            """;

    @Test
    void aCrashMidEmitIsRedoneAndTheFileCompletesOnceCp8() {
        String url = Containers.newDatabase("settlement_worker_it");
        String topic = "events-worker";
        Containers.createTopic(topic, 2);
        try (HikariDataSource pool = Containers.appPool(url, 4)) {
            PostgresStore store = new PostgresStore(pool, BalanceStrategy.APPEND_SNAPSHOT);
            PostgresDirectory directory = new PostgresDirectory(pool);
            SecretCipher cipher = new SecretCipher("it", Map.of("it", Base64.getEncoder().encodeToString(new byte[32])));
            Instant now = Instant.parse("2026-11-15T06:00:00Z");
            directory.onboard(new MerchantTerms(M, 1, new FeeSchedule(200, 1_800), 1, LocalDate.parse("2026-11-14")), now);
            directory.addWebhookSecret(new PostgresDirectory.WebhookSecret(M, "k1", cipher.encrypt(
                    "whsec_worker_0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8),
                    MerchantRegistry.context(M, "k1")), now, Optional.empty()));
            MerchantRegistry merchants = MerchantRegistry.load(directory, cipher, now);
            SettlementInbox inbox = new SettlementInbox(pool);
            DriftlineProperties.Kafka kafka = new DriftlineProperties.Kafka(Containers.KAFKA.getBootstrapServers(),
                    topic, 2, "matcher-worker-it", 50, Duration.ofMillis(100), Duration.ofSeconds(6),
                    Duration.ofSeconds(300), Duration.ofSeconds(10), Duration.ofSeconds(60));
            byte[] raw = FILE.getBytes(StandardCharsets.UTF_8);
            SettlementInbox.Stored stored = inbox.store(M, raw, now);
            assertThat(stored.isNew()).isTrue();
            assertThat(inbox.store(M, raw, now).isNew()).as("the same bytes again are the same upload").isFalse();

            try (KafkaEventPublisher publisher = new KafkaEventPublisher(
                    new KafkaProducer<>(KafkaClients.producer(kafka, "worker-it")), topic, Duration.ofSeconds(10))) {
                AtomicBoolean crashOnce = new AtomicBoolean(true);
                SettlementWorker crashing = new SettlementWorker(inbox, store, publisher, merchants, new SystemClock(),
                        point -> {
                            if (point == CrashPoint.CP8 && crashOnce.compareAndSet(true, false)) {
                                throw new IllegalStateException("simulated crash at CP8, after the emit");
                            }
                        }, Duration.ofMillis(100), 10);
                assertThatThrownBy(crashing::drainOnce).hasMessageContaining("CP8");
                assertThat(inbox.find(M, stored.sha256()).orElseThrow().state())
                        .isEqualTo(SettlementInbox.State.RECEIVED);

                SettlementWorker restarted = new SettlementWorker(inbox, store, publisher, merchants, new SystemClock(),
                        CrashPoints.NONE, Duration.ofMillis(100), 10);
                assertThat(restarted.drainOnce()).isEqualTo(1);
                assertThat(inbox.find(M, stored.sha256()).orElseThrow().state())
                        .isEqualTo(SettlementInbox.State.EMITTED);
                assertThat(restarted.drainOnce()).as("nothing left to do").isZero();
            }
            assertThat(count(url, "SELECT count(*) FROM settlement_files")).as("one receipt").isEqualTo(1);

            PartitionDriver driver = new PartitionDriver(store, new Matcher(merchants.directory()), new SystemClock(),
                    () -> new KafkaConsumer<>(KafkaClients.consumer(kafka, "worker-it-matcher")), topic, "worker-it",
                    CrashPoints.NONE, PartitionDriver.Listener.NONE, Duration.ofMillis(100), Duration.ofSeconds(60),
                    Duration.ofSeconds(30));
            driver.start();
            try {
                await("both emissions consumed", Duration.ofSeconds(60), () -> count(url,
                        "SELECT coalesce(max(next_offset), -1) FROM partition_leases WHERE partition = 1") == 6);
                assertThat(count(url, "SELECT count(*) FROM settlement_file_ends WHERE file_id = 'stl_worker_1'"
                        + " AND complete")).as("one COMPLETE row").isEqualTo(1);
                assertThat(count(url, "SELECT count(*) FROM settlement_lines WHERE file_id = 'stl_worker_1'"))
                        .isEqualTo(2);
                assertThat(count(url, "SELECT count(*) FROM processed_events WHERE source = 'BANK'"))
                        .as("three dedupe rows, though six records were read").isEqualTo(3);
            } finally {
                driver.stop();
            }
        }
    }
}
