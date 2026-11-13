package io.github.vanish073.driftline.core.event;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.money.Money;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

/**
 * Payload of {@code settlement.line}: one detail record of a settlement file (DESIGN §6.1). The file format itself
 * is fixed in Shift 5 ({@code docs/formats.md}). Only structural checks happen here; whether an amount makes sense
 * is the parser's and the matcher's call.
 */
public record SettlementLinePayload(
        String fileId,
        int lineNo,
        LocalDate settlementDate,
        LocalDate valueDate,
        Money amount,
        Optional<Money> fee,
        Optional<Money> tax,
        Optional<String> reference,
        Optional<String> batchId,
        String narration) implements EventPayload {

    public SettlementLinePayload {
        IdRules.requireToken("fileId", fileId);
        Objects.requireNonNull(settlementDate, "settlementDate");
        Objects.requireNonNull(valueDate, "valueDate");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(fee, "fee");
        Objects.requireNonNull(tax, "tax");
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(narration, "narration");
        if (lineNo < 1) {
            throw new IllegalArgumentException("line numbers start at 1, got " + lineNo);
        }
        fee.ifPresent(f -> requireNotNegative("fee", f));
        tax.ifPresent(t -> requireNotNegative("tax", t));
        reference.ifPresent(r -> IdRules.requireToken("reference", r));
        batchId.ifPresent(b -> IdRules.requireToken("batchId", b));
    }

    private static void requireNotNegative(String field, Money value) {
        if (value.isNegative()) {
            throw new IllegalArgumentException(field + " must not be negative");
        }
    }
}
