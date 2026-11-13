package io.github.vanish073.driftline.core.ledger;

import static io.github.vanish073.driftline.core.ledger.AccountCode.BANK;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_FEES;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GATEWAY_RECEIVABLE;
import static io.github.vanish073.driftline.core.ledger.AccountCode.GST_INPUT_CREDIT;
import static io.github.vanish073.driftline.core.ledger.AccountCode.PAYMENTS_CLEARING;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SALES;
import static io.github.vanish073.driftline.core.ledger.AccountCode.SUSPENSE;

import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The posting rules of DESIGN §7.2 as pure functions. Each returns a validated {@link LedgerTxn}, so arithmetic that
 * does not add up (for example received + fees + GST ≠ gross) is rejected here, at layer 1. Fee and GST entries of
 * zero are left out, because a zero entry is not allowed.
 */
public final class PostingRules {

    private PostingRules() {
    }

    /** {@code CAPTURE, paymentId}: Dr GATEWAY_RECEIVABLE +g · Cr PAYMENTS_CLEARING −g. */
    public static LedgerTxn capture(MerchantId m, PaymentId payment, Money gross, Instant at) {
        requirePositive("gross", gross);
        return LedgerTxn.of(m, new Cause(CauseType.CAPTURE, payment.value()), at, List.of(
                Entry.debit(m, GATEWAY_RECEIVABLE, gross),
                Entry.credit(m, PAYMENTS_CLEARING, gross)));
    }

    /** {@code ORDER_MATCH, paymentId}: Dr PAYMENTS_CLEARING +g · Cr SALES −g. */
    public static LedgerTxn orderMatch(MerchantId m, PaymentId payment, Money gross, Instant at) {
        requirePositive("gross", gross);
        return LedgerTxn.of(m, new Cause(CauseType.ORDER_MATCH, payment.value()), at, List.of(
                Entry.debit(m, PAYMENTS_CLEARING, gross),
                Entry.credit(m, SALES, gross)));
    }

    /**
     * {@code REFUND, paymentId}: Dr PAYMENTS_CLEARING +g (or SALES, if the payment was matched to an order) ·
     * Cr GATEWAY_RECEIVABLE −g.
     */
    public static LedgerTxn refund(MerchantId m, PaymentId payment, Money gross, boolean matchedToOrder, Instant at) {
        requirePositive("gross", gross);
        return LedgerTxn.of(m, new Cause(CauseType.REFUND, payment.value()), at, List.of(
                Entry.debit(m, matchedToOrder ? SALES : PAYMENTS_CLEARING, gross),
                Entry.credit(m, GATEWAY_RECEIVABLE, gross)));
    }

    /**
     * {@code SETTLEMENT, fileId:lineNo}: Dr BANK +received · Dr GATEWAY_FEES +Σfee · Dr GST_INPUT_CREDIT +Σgst ·
     * Cr GATEWAY_RECEIVABLE −Σgross. Requires received + fees + gst = gross.
     */
    public static LedgerTxn settlement(MerchantId m, String fileId, int lineNo, Money received, Money fees, Money gst,
                                       Money gross, Instant at) {
        requirePositive("received", received);
        requirePositive("gross", gross);
        List<Entry> entries = new ArrayList<>();
        entries.add(Entry.debit(m, BANK, received));
        addFeesAndGst(entries, m, fees, gst);
        entries.add(Entry.credit(m, GATEWAY_RECEIVABLE, gross));
        return LedgerTxn.of(m, lineCause(CauseType.SETTLEMENT, fileId, lineNo), at, entries);
    }

    /** {@code SETTLEMENT_PARTIAL, fileId:lineNo}: Dr BANK +received · Cr GATEWAY_RECEIVABLE −received. */
    public static LedgerTxn settlementPartial(MerchantId m, String fileId, int lineNo, Money received, Instant at) {
        requirePositive("received", received);
        return LedgerTxn.of(m, lineCause(CauseType.SETTLEMENT_PARTIAL, fileId, lineNo), at, List.of(
                Entry.debit(m, BANK, received),
                Entry.credit(m, GATEWAY_RECEIVABLE, received)));
    }

    /** {@code SUSPENSE_IN, fileId:lineNo}: Dr BANK +a · Cr SUSPENSE −a. */
    public static LedgerTxn suspenseIn(MerchantId m, String fileId, int lineNo, Money amount, Instant at) {
        requirePositive("amount", amount);
        return LedgerTxn.of(m, lineCause(CauseType.SUSPENSE_IN, fileId, lineNo), at, List.of(
                Entry.debit(m, BANK, amount),
                Entry.credit(m, SUSPENSE, amount)));
    }

    /**
     * {@code SUSPENSE_RELEASE, fileId:lineNo}: Dr SUSPENSE +a · Dr fees and GST · Cr GATEWAY_RECEIVABLE −gross.
     * Requires a + fees + gst = gross.
     */
    public static LedgerTxn suspenseRelease(MerchantId m, String fileId, int lineNo, Money amount, Money fees, Money gst,
                                            Money gross, Instant at) {
        requirePositive("amount", amount);
        requirePositive("gross", gross);
        List<Entry> entries = new ArrayList<>();
        entries.add(Entry.debit(m, SUSPENSE, amount));
        addFeesAndGst(entries, m, fees, gst);
        entries.add(Entry.credit(m, GATEWAY_RECEIVABLE, gross));
        return LedgerTxn.of(m, lineCause(CauseType.SUSPENSE_RELEASE, fileId, lineNo), at, entries);
    }

    /** {@code MANUAL_ADJUSTMENT, auditSeq}: as entered through the audited ops API; it must balance. */
    public static LedgerTxn manualAdjustment(MerchantId m, long auditSeq, List<Entry> entries, Instant at) {
        if (auditSeq < 1) {
            throw new IllegalArgumentException("audit sequence numbers start at 1, got " + auditSeq);
        }
        return LedgerTxn.of(m, new Cause(CauseType.MANUAL_ADJUSTMENT, Long.toString(auditSeq)), at, entries);
    }

    private static Cause lineCause(CauseType type, String fileId, int lineNo) {
        IdRules.requireToken("fileId", fileId);
        if (lineNo < 1) {
            throw new IllegalArgumentException("line numbers start at 1, got " + lineNo);
        }
        return new Cause(type, fileId + ":" + lineNo);
    }

    private static void addFeesAndGst(List<Entry> entries, MerchantId m, Money fees, Money gst) {
        requireNotNegative("fees", fees);
        requireNotNegative("gst", gst);
        if (fees.isPositive()) {
            entries.add(Entry.debit(m, GATEWAY_FEES, fees));
        }
        if (gst.isPositive()) {
            entries.add(Entry.debit(m, GST_INPUT_CREDIT, gst));
        }
    }

    private static void requirePositive(String field, Money value) {
        if (!value.isPositive()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    private static void requireNotNegative(String field, Money value) {
        if (value.isNegative()) {
            throw new IllegalArgumentException(field + " must not be negative");
        }
    }
}
