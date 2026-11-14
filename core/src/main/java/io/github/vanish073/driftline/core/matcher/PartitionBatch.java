package io.github.vanish073.driftline.core.matcher;

import java.util.List;
import java.util.Objects;

/** Consecutive records of one partition, processed in one Store transaction (DESIGN §5.7). */
public record PartitionBatch(int partition, List<LogRecord> records) {

    public PartitionBatch {
        Objects.requireNonNull(records, "records");
        records = List.copyOf(records);
        for (int i = 0; i < records.size(); i++) {
            LogRecord r = records.get(i);
            if (r.partition() != partition) {
                throw new IllegalArgumentException("record " + i + " is from partition " + r.partition());
            }
            if (i > 0 && r.offset() != records.get(i - 1).offset() + 1) {
                throw new IllegalArgumentException("offsets in a batch must be consecutive");
            }
        }
    }
}
