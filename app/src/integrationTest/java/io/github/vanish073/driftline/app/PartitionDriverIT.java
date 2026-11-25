package io.github.vanish073.driftline.app;

import static io.github.vanish073.driftline.app.Containers.await;
import static io.github.vanish073.driftline.app.Containers.count;
import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.github.vanish073.driftline.app.config.DriftlineProperties;
import io.github.vanish073.driftline.app.kafka.KafkaClients;
import io.github.vanish073.driftline.app.kafka.KafkaEventPublisher;
import io.github.vanish073.driftline.app.kafka.PartitionDriver;
import io.github.vanish073.driftline.app.observability.BacklogProbe;
import io.github.vanish073.driftline.app.port.SystemClock;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.OrderPayload;
import io.github.vanish073.driftline.core.event.PaymentPayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.OrderId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.matcher.BatchOutcome;
import io.github.vanish073.driftline.core.matcher.Matcher;
import io.github.vanish073.driftline.core.matcher.MerchantDirectory;
import io.github.vanish073.driftline.core.matcher.MerchantTerms;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import io.github.vanish073.driftline.store.postgres.BalanceStrategy;
import io.github.vanish073.driftline.store.postgres.PostgresStore;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * The Kafka partition driver against real Kafka and real PostgreSQL (DESIGN §5.1, §5.7, §9.4, §11.9): two consumers
 * and a rebalance, a zombie fenced (CP7), a crash mid-batch and the replay (CP6), and an undecodable record. Every test
 * ends by checking that each event had its effects exactly one time in the database: one dedupe row per event, one
 * decision per payment event, two ledger postings per paid order.
 */
class PartitionDriverIT {

    static final int PARTITIONS = 4;
    static final Instant T0 = Instant.parse("2026-11-14T05:00:00Z");
    static final List<MerchantId> MERCHANTS = List.of(MerchantId.of("m_0"), MerchantId.of("m_1"), MerchantId.of("m_2"),
            MerchantId.of("m_3"));
    static final MerchantDirectory DIRECTORY;

    static {
        List<MerchantTerms> terms = new ArrayList<>();
        for (int i = 0; i < MERCHANTS.size(); i++) {
            terms.add(new MerchantTerms(MERCHANTS.get(i), i, new FeeSchedule(200, 1_800), 1, LocalDate.parse("2026-11-15")));
        }
        DIRECTORY = new MerchantDirectory(terms);
    }

    String url;
    HikariDataSource pool;
    PostgresStore store;

    String topic;
    String group;
    KafkaEventPublisher publisher;
    final List<PartitionDriver> drivers = new ArrayList<>();
    int orders;

    /** A fresh database and topic per test: leases are per partition number, so they must not carry over. */
    @BeforeEach
    void topic(TestInfo test) {
        String name = test.getTestMethod().orElseThrow().getName();
        url = Containers.newDatabase("driver_" + Integer.toHexString(name.hashCode()));
        pool = Containers.appPool(url, 8);
        store = new PostgresStore(pool, BalanceStrategy.APPEND_SNAPSHOT);
        topic = "events-" + name;
        group = "matcher-" + name;
        Containers.createTopic(topic, PARTITIONS);
        publisher = new KafkaEventPublisher(new KafkaProducer<>(KafkaClients.producer(kafka(), "it-producer")), topic,
                Duration.ofSeconds(10));
    }

    @AfterEach
    void stop() {
        drivers.forEach(PartitionDriver::stop);
        publisher.close();
        pool.close();
    }

    static DriftlineProperties.Kafka kafka() {
        return new DriftlineProperties.Kafka(Containers.KAFKA.getBootstrapServers(), "unused", PARTITIONS, "unused", 50,
                Duration.ofMillis(100), Duration.ofSeconds(6), Duration.ofSeconds(300), Duration.ofSeconds(10),
                Duration.ofSeconds(60), "PLAINTEXT", null, null, null);
    }

    /** Merchant ids are shared across tests, so each test's orders get their own ids. */
    private String prefix() {
        return topic.substring("events-".length(), Math.min(topic.length(), "events-".length() + 12));
    }

    /** A paid order: created, a successful payment, paid with it. Four orders per call, one per partition. */
    private void produceRound() {
        List<EventEnvelope> out = new ArrayList<>();
        for (int p = 0; p < PARTITIONS; p++) {
            MerchantId m = MERCHANTS.get(p);
            String o = prefix() + "-ord-" + orders;
            String pay = prefix() + "-pay-" + orders;
            Instant at = T0.plusSeconds(orders * 10L);
            OrderPayload created = new OrderPayload(OrderId.of(o), 1, Money.ofPaise(10_000), at.plusSeconds(900),
                    Optional.empty());
            OrderPayload paid = new OrderPayload(OrderId.of(o), 2, Money.ofPaise(10_000), at.plusSeconds(900),
                    Optional.of(PaymentId.of(pay)));
            out.add(envelope(m, p, Source.MERCHANT, o + "-v1", EventType.ORDER_CREATED, at, created));
            out.add(envelope(m, p, Source.GATEWAY, "evt-" + pay, EventType.PAYMENT_SUCCEEDED, at.plusSeconds(2),
                    new PaymentPayload(PaymentId.of(pay), OrderId.of(o), Money.ofPaise(10_000))));
            out.add(envelope(m, p, Source.MERCHANT, o + "-v2", EventType.ORDER_PAID, at.plusSeconds(3), paid));
        }
        orders++;
        publisher.publish(out);
    }

    private static EventEnvelope envelope(MerchantId m, int p, Source source, String key, EventType type, Instant at,
                                          io.github.vanish073.driftline.core.event.EventPayload payload) {
        return new EventEnvelope(EventEnvelope.SCHEMA_VERSION, m, p, source, key, type, at, at,
                Sha256.of(key.getBytes(StandardCharsets.UTF_8)), payload, Optional.empty());
    }

    private PartitionDriver driver(String name, CrashPoints crash, PartitionDriver.Listener listener,
                                   Map<String, String> overrides) {
        return driver(name, crash, listener, overrides, Duration.ofSeconds(30));
    }

    private PartitionDriver driver(String name, CrashPoints crash, PartitionDriver.Listener listener,
                                   Map<String, String> overrides, Duration reacquireAfter) {
        Properties props = KafkaClients.consumer(new DriftlineProperties.Kafka(Containers.KAFKA.getBootstrapServers(),
                topic, PARTITIONS, group, 50, Duration.ofMillis(100), Duration.ofSeconds(6), Duration.ofSeconds(300),
                Duration.ofSeconds(10), Duration.ofSeconds(60), "PLAINTEXT", null, null, null), name);
        props.putAll(overrides);
        PartitionDriver d = new PartitionDriver(store, new Matcher(DIRECTORY), new SystemClock(),
                () -> new KafkaConsumer<>(props), topic, name, crash, listener, Duration.ofMillis(100),
                Duration.ofSeconds(60), reacquireAfter);
        drivers.add(d);
        d.start();
        return d;
    }

    private PartitionDriver driver(String name) {
        return driver(name, CrashPoints.NONE, PartitionDriver.Listener.NONE, Map.of());
    }

    /** Effects of this test's events, from the database. */
    private long dedupeRows() {
        return count(url, "SELECT count(*) FROM processed_events WHERE event_key LIKE '%" + prefix() + "-%'");
    }

    private long paymentDecisions() {
        return count(url, "SELECT count(*) FROM payment_transitions WHERE payment_id LIKE '" + prefix() + "-%'");
    }

    private long postings() {
        return count(url, "SELECT count(*) FROM ledger_transactions WHERE cause_id LIKE '%" + prefix() + "-%'");
    }

    private void assertEffectsAppliedOnce() {
        await("every event applied", Duration.ofSeconds(60), () -> dedupeRows() == 3L * PARTITIONS * orders
                && postings() == 2L * PARTITIONS * orders);
        assertThat(dedupeRows()).as("one dedupe row per event").isEqualTo(3L * PARTITIONS * orders);
        assertThat(paymentDecisions()).as("one decision per payment event").isEqualTo((long) PARTITIONS * orders);
        assertThat(postings()).as("CAPTURE and ORDER_MATCH once per paid order").isEqualTo(2L * PARTITIONS * orders);
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void twoConsumersShareThePartitionsAndARebalanceLosesOrRepeatsNothing() {
        produceRound();
        PartitionDriver a = driver("matcher-a");
        await("a owns every partition", Duration.ofSeconds(60), () -> a.ownedPartitions().size() == PARTITIONS);
        assertEffectsAppliedOnce();

        PartitionDriver b = driver("matcher-b");
        await("b takes a share", Duration.ofSeconds(60), () -> !b.ownedPartitions().isEmpty()
                && a.ownedPartitions().size() + b.ownedPartitions().size() == PARTITIONS);
        assertThat(a.ownedPartitions()).doesNotContainAnyElementsOf(b.ownedPartitions());
        for (Integer p : b.ownedPartitions()) {
            assertThat(count(url, "SELECT epoch FROM partition_leases WHERE partition = " + p))
                    .as("a moved partition was acquired again, bumping its epoch").isGreaterThanOrEqualTo(2);
        }
        produceRound();
        produceRound();
        assertEffectsAppliedOnce();

        b.stop();
        await("a takes everything back", Duration.ofSeconds(60), () -> a.ownedPartitions().size() == PARTITIONS);
        produceRound();
        assertEffectsAppliedOnce();
        assertThat(a.failure()).isEmpty();
    }

    @Test
    void aZombieThatWakesAfterReassignmentIsFencedAndWritesNothingCp7() throws InterruptedException {
        produceRound();
        AtomicBoolean armed = new AtomicBoolean();
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CrashPoints pauseAtCp7 = point -> {
            if (point == CrashPoint.CP7 && armed.compareAndSet(true, false)) {
                paused.countDown();
                try {
                    release.await(60, TimeUnit.SECONDS);   // a stop-the-world pause holding a fetched batch
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        AtomicInteger fenced = new AtomicInteger();
        PartitionDriver.Listener countFences = new PartitionDriver.Listener() {
            @Override
            public void fenced(int partition) {
                fenced.incrementAndGet();
            }
        };
        // A short poll interval: a member that stops polling for 4 s is removed from the group.
        PartitionDriver a = driver("matcher-a", pauseAtCp7, countFences,
                Map.of(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "4000"));
        await("a owns every partition", Duration.ofSeconds(60), () -> a.ownedPartitions().size() == PARTITIONS);
        assertEffectsAppliedOnce();

        PartitionDriver b = driver("matcher-b");
        await("both hold partitions", Duration.ofSeconds(60), () -> !b.ownedPartitions().isEmpty()
                && !a.ownedPartitions().isEmpty());
        armed.set(true);
        produceRound();
        assertThat(paused.await(60, TimeUnit.SECONDS)).as("a paused holding a batch").isTrue();
        await("b took every partition from the paused member", Duration.ofSeconds(60),
                () -> b.ownedPartitions().size() == PARTITIONS);
        assertEffectsAppliedOnce();

        release.countDown();
        await("the zombie's held batch is fenced", Duration.ofSeconds(60), () -> fenced.get() >= 1);
        assertEffectsAppliedOnce();
        produceRound();
        assertEffectsAppliedOnce();
    }

    private static long total(Map<Integer, Long> lag) {
        return lag.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * D83: while the driver is stuck inside a chunk, the backlog probe counts every record that arrives and is not
     * processed; the Kafka client's own estimate, which only the stuck loop reports, never sees them.
     */
    @Test
    void theBacklogProbeCountsRecordsPilingUpWhileTheDriverIsStuck() throws InterruptedException {
        AtomicBoolean armed = new AtomicBoolean();
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CrashPoints stallAtCp7 = point -> {
            if (point == CrashPoint.CP7 && armed.compareAndSet(true, false)) {
                paused.countDown();
                try {
                    release.await(60, TimeUnit.SECONDS);   // a matcher stuck before a chunk's transaction
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        TreeMap<Integer, Long> clientLag = new TreeMap<>();
        PartitionDriver.Listener recordClientLag = new PartitionDriver.Listener() {
            @Override
            public void lag(int partition, long records) {
                synchronized (clientLag) {
                    clientLag.put(partition, records);
                }
            }
        };
        produceRound();
        PartitionDriver s = driver("matcher-s", stallAtCp7, recordClientLag, Map.of());
        await("s owns every partition", Duration.ofSeconds(60), () -> s.ownedPartitions().size() == PARTITIONS);
        assertEffectsAppliedOnce();
        try (Admin admin = Admin.create(KafkaClients.admin(kafka(), "it-backlog"))) {
            BacklogProbe.EndOffsets ends = partitions -> KafkaClients.endOffsets(admin, topic, partitions,
                    Duration.ofSeconds(10));
            BacklogProbe.StoredOffsets stored = BacklogProbe.storedOffsets(store);
            Supplier<Long> backlog = () -> total(BacklogProbe.measure(s.ownedPartitions(), ends, stored));
            await("no backlog once every event is applied", Duration.ofSeconds(30), () -> backlog.get() == 0);

            armed.set(true);
            produceRound();
            assertThat(paused.await(60, TimeUnit.SECONDS)).as("the driver is stuck").isTrue();
            produceRound();
            produceRound();
            long piled = 3L * 3 * PARTITIONS;   // three rounds of three events per partition, none processed
            await("the probe counts every record not processed", Duration.ofSeconds(30),
                    () -> backlog.get() == piled);
            synchronized (clientLag) {
                assertThat(total(clientLag)).as("the client's estimate, reported only by the stuck loop")
                        .isLessThan(piled);
            }

            release.countDown();
            assertEffectsAppliedOnce();
            await("the backlog drains", Duration.ofSeconds(30), () -> backlog.get() == 0);
            assertThat(s.failure()).isEmpty();
        } finally {
            release.countDown();
        }
    }

    /** BUG-003, D50: a zombie bumps the epoch of a partition Kafka still assigns here; the driver takes it back. */
    @Test
    void aFencedPartitionStillAssignedHereIsAcquiredAgainAndNothingIsLost() {
        produceRound();
        AtomicInteger fenced = new AtomicInteger();
        PartitionDriver.Listener countFences = new PartitionDriver.Listener() {
            @Override
            public void fenced(int partition) {
                fenced.incrementAndGet();
            }
        };
        PartitionDriver a = driver("matcher-a", CrashPoints.NONE, countFences, Map.of(), Duration.ofSeconds(2));
        await("a owns every partition", Duration.ofSeconds(60), () -> a.ownedPartitions().size() == PARTITIONS);
        assertEffectsAppliedOnce();
        long before = count(url, "SELECT epoch FROM partition_leases WHERE partition = 0");

        store.transact("zombie-acquire-p0", tx -> tx.partitions().acquire(0, "zombie"));
        produceRound();
        await("a is fenced on p0", Duration.ofSeconds(60), () -> fenced.get() >= 1);
        await("a acquires p0 again", Duration.ofSeconds(60),
                () -> count(url, "SELECT epoch FROM partition_leases WHERE partition = 0") >= before + 2);
        assertEffectsAppliedOnce();
        assertThat(a.failure()).isEmpty();
    }

    @Test
    void aCrashMidBatchRollsBackAndTheReplayAppliesEachEventOnceCp6() {
        produceRound();
        produceRound();
        AtomicBoolean crashOnce = new AtomicBoolean(true);
        PartitionDriver c = driver("matcher-c", point -> {
            if (point == CrashPoint.CP6 && crashOnce.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated crash at CP6, before the batch commits");
            }
        }, PartitionDriver.Listener.NONE, Map.of());
        await("the driver died", Duration.ofSeconds(60), () -> c.failure().isPresent());
        assertThat(c.failure().get()).hasMessageContaining("CP6");
        long afterCrash = dedupeRows();
        assertThat(afterCrash).as("the crashed batch left nothing behind").isLessThan(3L * PARTITIONS * orders);

        PartitionDriver d = driver("matcher-d");   // the restarted process: a new member, a new epoch
        await("d owns every partition", Duration.ofSeconds(60), () -> d.ownedPartitions().size() == PARTITIONS);
        assertEffectsAppliedOnce();
        assertThat(d.failure()).isEmpty();
    }

    @Test
    void anUndecodableRecordIsQuarantinedWithItsBytesAndThePartitionMovesOn() throws Exception {
        produceRound();
        byte[] garbage = "{\"schema_version\":1,\"this is\":\"not an envelope\"}".getBytes(StandardCharsets.UTF_8);
        try (KafkaProducer<byte[], byte[]> raw = new KafkaProducer<>(KafkaClients.producer(kafka(), "it-raw"))) {
            raw.send(new ProducerRecord<>(topic, 2, "m_2".getBytes(StandardCharsets.UTF_8), garbage)).get();
        }
        produceRound();
        List<BatchOutcome> seen = new ArrayList<>();
        AtomicInteger quarantined = new AtomicInteger();
        PartitionDriver e = driver("matcher-e", CrashPoints.NONE, new PartitionDriver.Listener() {
            @Override
            public void committed(int partition, List<BatchOutcome> outcomes, int q) {
                synchronized (seen) {
                    seen.addAll(outcomes);
                }
                quarantined.addAndGet(q);
            }
        }, Map.of());
        assertEffectsAppliedOnce();
        await("the bad record is quarantined", Duration.ofSeconds(30), () -> quarantined.get() == 1);
        assertThat(count(url, "SELECT count(*) FROM quarantine WHERE reason = 'UNDECODABLE' AND partition = 2"
                + " AND raw = decode('" + java.util.HexFormat.of().formatHex(garbage) + "', 'hex')")).isEqualTo(1);
        assertThat(count(url, "SELECT next_offset FROM partition_leases WHERE partition = 2"))
                .as("offsets 0-2 events, 3 garbage, 4-6 events").isEqualTo(7);
        assertThat(e.failure()).isEmpty();
    }
}
