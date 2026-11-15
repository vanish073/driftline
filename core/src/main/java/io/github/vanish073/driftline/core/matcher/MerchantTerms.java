package io.github.vanish073.driftline.core.matcher;

import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.settlement.FeeSchedule;
import java.time.LocalDate;
import java.util.Objects;

/**
 * What the matcher needs to know about a merchant (DESIGN §12 {@code merchants}): its partition, pricing, settlement
 * cycle and settlement calendar. Calendar v1 is {@code DAILY} from {@code firstSettlementDate}: every day from then on
 * is expected to have one settlement file (docs/formats.md).
 *
 * @param cycleDays the settlement cycle: a payment captured on IST day T is due in the file for T + cycleDays
 */
public record MerchantTerms(MerchantId merchantId, int partition, FeeSchedule fees, int cycleDays,
                            LocalDate firstSettlementDate) {

    public MerchantTerms {
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(fees, "fees");
        Objects.requireNonNull(firstSettlementDate, "firstSettlementDate");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }
        if (cycleDays < 0 || cycleDays > 30) {
            throw new IllegalArgumentException("cycleDays must be 0 to 30");
        }
    }

    /** The due date of a payment whose T date is {@code tDate}. */
    public LocalDate dueDate(LocalDate tDate) {
        return tDate.plusDays(cycleDays);
    }
}
