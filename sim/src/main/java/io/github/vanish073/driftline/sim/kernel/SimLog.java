package io.github.vanish073.driftline.sim.kernel;

import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.matcher.LogRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/** Kafka in the simulator (DESIGN §3.2, §11.3): partitions of records with offsets. Appends are durable. */
public final class SimLog {

    private final TreeMap<Integer, List<LogRecord>> partitions = new TreeMap<>();

    public SimLog(int partitionCount) {
        for (int p = 0; p < partitionCount; p++) {
            partitions.put(p, new ArrayList<>());
        }
    }

    public LogRecord append(EventEnvelope envelope) {
        List<LogRecord> records = partition(envelope.partition());
        LogRecord record = new LogRecord(envelope.partition(), records.size(), envelope);
        records.add(record);
        return record;
    }

    /** Up to {@code max} records of {@code partition} starting at {@code fromOffset}. */
    public List<LogRecord> read(int partition, long fromOffset, int max) {
        List<LogRecord> records = partition(partition);
        int from = Math.toIntExact(Math.min(fromOffset, records.size()));
        int to = Math.min(records.size(), from + max);
        return List.copyOf(records.subList(from, to));
    }

    public long endOffset(int partition) {
        return partition(partition).size();
    }

    public int partitionCount() {
        return partitions.size();
    }

    private List<LogRecord> partition(int partition) {
        List<LogRecord> records = partitions.get(partition);
        if (records == null) {
            throw new IllegalArgumentException("no partition " + partition);
        }
        return records;
    }
}
