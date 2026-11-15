package io.github.vanish073.driftline.wire.settlement;

import io.github.vanish073.driftline.core.event.SettlementFileEndPayload;
import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * One Driftline Settlement File v1 (DESIGN §6.1, {@code docs/formats.md}): the header fields, the detail lines as
 * {@code settlement.line} payloads, and the trailer total. The constructor enforces what the trailer promises (line
 * numbers 1..N, every line belonging to this file, and the exact total), so a file that exists in memory is always
 * internally consistent, whether it was parsed or is about to be written.
 */
public record SettlementFile(String fileId, MerchantId merchantId, LocalDate settlementDate, Instant generatedAt,
                             List<SettlementLinePayload> lines, Money totalAmount) {

    public SettlementFile {
        IdRules.requireToken("fileId", fileId);
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(settlementDate, "settlementDate");
        Objects.requireNonNull(generatedAt, "generatedAt");
        Objects.requireNonNull(totalAmount, "totalAmount");
        lines = List.copyOf(lines);
        if (generatedAt.getNano() % 1_000 != 0) {
            throw new IllegalArgumentException("generatedAt has sub-microsecond precision");
        }
        Money sum = Money.ZERO;
        for (int i = 0; i < lines.size(); i++) {
            SettlementLinePayload line = lines.get(i);
            if (line.lineNo() != i + 1) {
                throw new IllegalArgumentException("line " + (i + 1) + " is numbered " + line.lineNo());
            }
            if (!line.fileId().equals(fileId) || !line.settlementDate().equals(settlementDate)) {
                throw new IllegalArgumentException("line " + line.lineNo() + " belongs to another file");
            }
            if (line.amount().isNegative()) {
                throw new IllegalArgumentException("line " + line.lineNo() + " has a negative amount");
            }
            sum = sum.plus(line.amount());
        }
        if (!sum.equals(totalAmount)) {
            throw new IllegalArgumentException("the lines sum to " + sum + ", not the total " + totalAmount);
        }
    }

    /** A file with these lines, whose total is their exact sum. */
    public static SettlementFile of(String fileId, MerchantId merchantId, LocalDate settlementDate, Instant generatedAt,
                                    List<SettlementLinePayload> lines) {
        Money sum = Money.ZERO;
        for (SettlementLinePayload line : lines) {
            sum = sum.plus(line.amount());
        }
        return new SettlementFile(fileId, merchantId, settlementDate, generatedAt, lines, sum);
    }

    /** The {@code settlement.file_end} payload for this file, whose raw bytes hash to {@code sha256}. */
    public SettlementFileEndPayload fileEnd(Sha256 sha256) {
        return new SettlementFileEndPayload(fileId, settlementDate, lines.size(), totalAmount, sha256);
    }
}
