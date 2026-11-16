package io.github.vanish073.driftline.app.kafka;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.wire.envelope.EnvelopeCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Publishes envelopes to {@code driftline.events.v1} (§3.3, §10.4): the partition is the merchant's, set explicitly on
 * every record (D5), the key is the merchant id, and a valid {@code traceparent} travels as a header (§13). It returns
 * only after Kafka acknowledged every record, so ingest answers 202 only for events that are in the log.
 */
public final class KafkaEventPublisher implements EventPublisher, AutoCloseable {

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final Duration ackTimeout;

    public KafkaEventPublisher(Producer<byte[], byte[]> producer, String topic, Duration ackTimeout) {
        this.producer = producer;
        this.topic = topic;
        this.ackTimeout = ackTimeout;
    }

    /**
     * Checks that the topic exists with exactly {@code expected} partitions and that every merchant's partition is in
     * range; otherwise ingest would write a merchant's events to a partition no matcher expects.
     */
    public void verifyTopic(int expected, int maxMerchantPartition) {
        List<PartitionInfo> partitions = producer.partitionsFor(topic);
        if (partitions.size() != expected) {
            throw new IllegalStateException("topic " + topic + " has " + partitions.size() + " partitions, but "
                    + "driftline.kafka.partitions is " + expected + "; re-partitioning is an offline procedure (§3.3)");
        }
        if (maxMerchantPartition >= expected) {
            throw new IllegalStateException("a merchant is onboarded to partition " + maxMerchantPartition
                    + ", but the topic has " + expected);
        }
    }

    @Override
    public void publish(List<EventEnvelope> envelopes) {
        List<Future<RecordMetadata>> acks = new ArrayList<>(envelopes.size());
        long deadline = System.nanoTime() + ackTimeout.toNanos();
        try {
            for (EventEnvelope e : envelopes) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, e.partition(),
                        e.merchantId().value().getBytes(StandardCharsets.UTF_8), EnvelopeCodec.encode(e));
                e.traceparent().ifPresent(t -> record.headers()
                        .add(new RecordHeader("traceparent", t.getBytes(StandardCharsets.US_ASCII))));
                acks.add(producer.send(record));
            }
            for (Future<RecordMetadata> ack : acks) {
                ack.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventLogUnavailable("interrupted while waiting for the event log", e);
        } catch (ExecutionException | TimeoutException | KafkaException e) {
            throw new EventLogUnavailable("the event log did not acknowledge in time", e);
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
    }
}
