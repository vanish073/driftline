package io.github.vanish073.driftline.wire.settlement;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.event.EventEnvelope;
import io.github.vanish073.driftline.core.event.EventType;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.event.Source;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.SettlementRepo.Receipt;
import io.github.vanish073.driftline.core.port.SettlementRepo.ReceiptStatus;
import io.github.vanish073.driftline.core.port.SettlementRepo.ReceivedFile;
import io.github.vanish073.driftline.core.port.Tx;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The settlement worker's two steps (DESIGN §6.2, D38), shared by the simulator's worker node and the production
 * worker in {@code app} so both run the same code (D1):
 * <ol>
 *   <li>{@link #receive}, inside one Store transaction: record the receipt of a parsed file. The same bytes again are
 *       a {@link Receipt#DUPLICATE}; a different file for a date or file id that already has one is
 *       {@link Receipt#CONFLICT}, stored as REJECTED_CONFLICT with a FILE_CONFLICT anomaly; the same refused bytes
 *       again stay refused (BUG-002).</li>
 *   <li>{@link #envelopes}: what to emit for a NEW or DUPLICATE receipt: {@code settlement.line} × N then
 *       {@code settlement.file_end}, keyed {@code sha256:lineNo} and {@code sha256:T}. A worker that is not sure an
 *       earlier emission finished emits again; the matcher's dedupe absorbs the repeats.</li>
 * </ol>
 */
public final class SettlementIntake {

    private SettlementIntake() {
    }

    public static Receipt receive(Tx tx, SettlementFile file, Sha256 sha, Instant now) {
        MerchantId merchantId = file.merchantId();
        Receipt r = tx.settlement().receive(new ReceivedFile(merchantId, file.fileId(), file.settlementDate(),
                sha, file.lines().size(), file.totalAmount(), now, ReceiptStatus.ACCEPTED));
        if (r == Receipt.DUPLICATE
                && tx.settlement().receipt(sha).orElseThrow().status() == ReceiptStatus.REJECTED_CONFLICT) {
            return Receipt.CONFLICT;   // the same refused bytes again: still refused, never emitted (BUG-002)
        }
        if (r == Receipt.CONFLICT) {
            TreeMap<String, String> facts = new TreeMap<>();
            facts.put("file_id", file.fileId());
            facts.put("settlement_date", file.settlementDate().toString());
            facts.put("sha256", sha.hex());
            tx.breaks().raise(Break.detected(merchantId, BreakType.FILE_CONFLICT,
                    file.settlementDate() + ":" + sha.hex().substring(0, 16), now, facts), "upload refused");
        }
        return r;
    }

    /** The log records of one accepted file, in emission order, for the merchant's partition. */
    public static List<EventEnvelope> envelopes(int partition, SettlementFile file, Sha256 sha, Instant receivedAt) {
        List<EventEnvelope> out = new ArrayList<>(file.lines().size() + 1);
        for (SettlementLinePayload line : file.lines()) {
            String key = sha.hex() + ":" + line.lineNo();
            out.add(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, file.merchantId(), partition, Source.BANK,
                    key, EventType.SETTLEMENT_LINE, file.generatedAt(), receivedAt,
                    Sha256.of(key.getBytes(StandardCharsets.US_ASCII)), line, Optional.empty()));
        }
        String endKey = sha.hex() + ":T";
        out.add(new EventEnvelope(EventEnvelope.SCHEMA_VERSION, file.merchantId(), partition, Source.BANK,
                endKey, EventType.SETTLEMENT_FILE_END, file.generatedAt(), receivedAt,
                Sha256.of(endKey.getBytes(StandardCharsets.US_ASCII)), file.fileEnd(sha), Optional.empty()));
        return List.copyOf(out);
    }
}
