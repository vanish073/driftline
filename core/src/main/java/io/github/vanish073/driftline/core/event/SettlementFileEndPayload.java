package io.github.vanish073.driftline.core.event;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.money.Money;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Payload of {@code settlement.file_end}: the trailer of a settlement file (DESIGN §6.1, §6.2). With it the
 * matcher can tell when it has seen lines 1..{@code lineCount}, which is what lets the settlement watermark
 * advance.
 */
public record SettlementFileEndPayload(
        String fileId, LocalDate settlementDate, int lineCount, Money totalAmount, Sha256 fileSha256)
        implements EventPayload {

    public SettlementFileEndPayload {
        IdRules.requireToken("fileId", fileId);
        Objects.requireNonNull(settlementDate, "settlementDate");
        Objects.requireNonNull(totalAmount, "totalAmount");
        Objects.requireNonNull(fileSha256, "fileSha256");
        if (lineCount < 0) {
            throw new IllegalArgumentException("lineCount must not be negative, got " + lineCount);
        }
    }
}
