package io.github.vanish073.driftline.core.matcher;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Consecutive records of one partition, processed in one Store transaction (DESIGN §5.7).
 *
 * @param epoch the partition epoch the processing instance holds; the offset commit is fenced by it (§5.1)
 * @param now   the driver's processing time (its {@code Clock}): idle watermarks, processing-time timers and
 *              {@code detectedAt} use it; state machines never do (§4.2)
 */
public record PartitionBatch(int partition, long epoch, Instant now, List<LogRecord> records) {

    public PartitionBatch {
        Objects.requireNonNull(now, "now");
        if (epoch < 1) {
            throw new IllegalArgumentException("a batch is processed under an acquired epoch (>= 1), not " + epoch);
        }
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
