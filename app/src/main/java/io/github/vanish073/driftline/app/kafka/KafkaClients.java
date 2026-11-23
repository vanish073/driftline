package io.github.vanish073.driftline.app.kafka;

import io.github.vanish073.driftline.app.config.DriftlineProperties;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** The Kafka client settings of DESIGN §10.4, in one place. */
public final class KafkaClients {

    private KafkaClients() {
    }

    /**
     * {@code acks=all}, idempotence on, at most 5 requests in flight, lz4. Idempotence removes producer-retry duplicates
     * within one producer session; correctness never depends on it (§2.2), because the consumer dedupes.
     */
    public static Properties producer(DriftlineProperties.Kafka kafka, String clientId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        long ack = kafka.ackTimeout().toMillis();
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(Math.max(1_000, ack / 2)));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Long.toString(ack + 1_000));
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(ack));
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return p;
    }

    /**
     * No auto-commit (offsets live in PostgreSQL, D2), the cooperative-sticky assignor and the classic group protocol.
     * The driver always seeks to the stored offset before it resumes a partition, so {@code auto.offset.reset} only
     * matters in between; {@code earliest} can at worst replay records, which the matcher skips by offset. A log that
     * lost records Driftline never processed (retention ran past the stored offset) cannot slip through either: the
     * matcher refuses a batch that skips offsets, and the driver stops loudly (D50).
     */
    public static Properties consumer(DriftlineProperties.Kafka kafka, String clientId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.groupId());
        p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(kafka.maxPollRecords()));
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Long.toString(kafka.sessionTimeout().toMillis()));
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Long.toString(Math.max(500, kafka.sessionTimeout().toMillis() / 3)));
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Long.toString(kafka.maxPollInterval().toMillis()));
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return p;
    }

    /** An admin client, for the backlog probe's end offsets (D83). */
    public static Properties admin(DriftlineProperties.Kafka kafka, String clientId) {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        long ack = kafka.ackTimeout().toMillis();
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(Math.max(1_000, ack / 2)));
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Long.toString(ack));
        return p;
    }

    /**
     * The end offset (the offset the next record will get) of each of {@code partitions} of {@code topic}, as the
     * {@code read_uncommitted} consumer sees it; waits at most {@code timeout}.
     *
     * @throws IllegalStateException if Kafka did not answer in time or refused
     */
    public static Map<Integer, Long> endOffsets(Admin admin, String topic, Collection<Integer> partitions,
                                                Duration timeout) {
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (Integer p : partitions) {
            request.put(new TopicPartition(topic, p), OffsetSpec.latest());
        }
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result = admin.listOffsets(request).all()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            TreeMap<Integer, Long> out = new TreeMap<>();
            result.forEach((tp, info) -> out.put(tp.partition(), info.offset()));
            return out;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while listing the end offsets of " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("cannot list the end offsets of " + topic + ": " + e, e);
        }
    }
}
