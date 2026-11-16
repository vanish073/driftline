package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.event.Sha256;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.id.PaymentId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Settlement state (DESIGN §6.2, §6.3, §12). Two writers, two parts:
 * <ul>
 *   <li>the <b>settlement worker</b> records each upload it accepts ({@link #receive}): duplicates of a file are a
 *       no-op, and a second, different file for the same settlement date is refused as a conflict;</li>
 *   <li>the <b>matcher</b> records what it read from the log: each line and how it was settled, each trailer and
 *       whether its file is COMPLETE, and each payment's settlement match. A payment can be matched at most once.</li>
 * </ul>
 * The matcher's state comes only from the log, never from the worker's rows, so replaying the log rebuilds it.
 */
public interface SettlementRepo {

    // ---- the worker's receipts -------------------------------------------------------------------------------------

    /** A parsed upload. Rejected uploads are stored too, so a conflict stays visible. */
    record ReceivedFile(MerchantId merchantId, String fileId, LocalDate settlementDate, Sha256 sha256, int lineCount,
                        Money totalAmount, Instant receivedAt, ReceiptStatus status) {

        public ReceivedFile {
            Objects.requireNonNull(merchantId, "merchantId");
            IdRules.requireToken("fileId", fileId);
            Objects.requireNonNull(settlementDate, "settlementDate");
            Objects.requireNonNull(sha256, "sha256");
            Objects.requireNonNull(totalAmount, "totalAmount");
            Objects.requireNonNull(receivedAt, "receivedAt");
            Objects.requireNonNull(status, "status");
            if (lineCount < 0) {
                throw new IllegalArgumentException("lineCount must not be negative");
            }
        }
    }

    enum ReceiptStatus {
        /** Accepted; its lines and trailer go to the log. */
        ACCEPTED,
        /** Refused: the merchant already has a different file for this date or with this file id (§6.2). */
        REJECTED_CONFLICT
    }

    /** What {@link #receive} found. */
    enum Receipt {
        /** First time this file was seen: recorded as ACCEPTED. */
        NEW,
        /** The same bytes again (same SHA-256): nothing written. */
        DUPLICATE,
        /** A different file for the same date or file id: recorded as REJECTED_CONFLICT. */
        CONFLICT
    }

    /**
     * Records an upload. {@code file.status()} is ignored; the store decides it.
     *
     * @return what was found; for {@link Receipt#DUPLICATE} the stored row is unchanged
     */
    Receipt receive(ReceivedFile file);

    /** The stored receipt of the file with this SHA-256, if any. */
    Optional<ReceivedFile> receipt(Sha256 sha256);

    // ---- the matcher's view ----------------------------------------------------------------------------------------

    /**
     * How a line was settled (§6.3, §6.4, §6.5). Every status but MATCHED, SHORT_EXPLAINED and INFERRED holds money in
     * SUSPENSE (SHORT_UNEXPLAINED holds only what it could not attribute).
     */
    enum LineStatus {
        /** Referenced, and received == expected net. */
        MATCHED,
        /** Referenced and short, and the itemised fee and tax explain the gap exactly: auto-resolved. */
        SHORT_EXPLAINED,
        /** Referenced and short, with no itemisation or itemisation that does not add up: a partial posting. */
        SHORT_UNEXPLAINED,
        /** Referenced, and received > expected net: an anomaly, held in suspense. */
        OVER,
        /** References a payment another line already settled: an anomaly, held in suspense. */
        CONFLICT,
        /** References a payment not known as captured (yet): an orphan, held in suspense until attributed. */
        UNATTRIBUTED,
        /** Carries no reference: waits for batch matching (§6.4), held in suspense. */
        AWAITING_BATCH,
        /** No reference, and exactly one subset of the candidates sums to it: MATCHED_INFERRED, with its candidate hash. */
        INFERRED,
        /** No reference, and two or more subsets fit: an orphan with two witnesses, held in suspense. */
        AMBIGUOUS,
        /** No reference, and no subset of the candidates fits: an orphan, held in suspense. */
        NO_SOLUTION,
        /** No reference, and the exact search needs more than the budget: an orphan, held in suspense. */
        BUDGET_EXCEEDED,
        /** No reference and a zero amount: rejected and flagged; it settles nothing. */
        ZERO_AMOUNT,
        /** No reference, and the amounts are too large to sum exactly: an orphan, held in suspense. */
        OUT_OF_RANGE
    }

    record Line(MerchantId merchantId, String fileId, int lineNo, LocalDate settlementDate, LocalDate valueDate,
                Money amount, Optional<Money> fee, Optional<Money> tax, Optional<String> reference,
                Optional<String> batchId, LineStatus status, Optional<Sha256> candidateHash,
                List<List<PaymentId>> witnesses) {

        public Line {
            Objects.requireNonNull(merchantId, "merchantId");
            IdRules.requireToken("fileId", fileId);
            Objects.requireNonNull(settlementDate, "settlementDate");
            Objects.requireNonNull(valueDate, "valueDate");
            Objects.requireNonNull(amount, "amount");
            Objects.requireNonNull(fee, "fee");
            Objects.requireNonNull(tax, "tax");
            Objects.requireNonNull(reference, "reference");
            Objects.requireNonNull(batchId, "batchId");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(candidateHash, "candidateHash");
            if ((status == LineStatus.INFERRED) != candidateHash.isPresent()) {
                throw new IllegalArgumentException("exactly the INFERRED lines carry a candidate hash");
            }
            witnesses = witnesses.stream().map(List::copyOf).toList();
            if ((status == LineStatus.AMBIGUOUS) != (witnesses.size() == 2)) {
                throw new IllegalArgumentException("exactly the AMBIGUOUS lines carry two witnesses");
            }
            if (witnesses.size() == 2 && (witnesses.get(0).isEmpty() || witnesses.get(0).equals(witnesses.get(1)))) {
                throw new IllegalArgumentException("the two witnesses must be non-empty and distinct");
            }
            if (lineNo < 1) {
                throw new IllegalArgumentException("line numbers start at 1");
            }
            if (amount.isNegative() || fee.filter(Money::isNegative).isPresent() || tax.filter(Money::isNegative).isPresent()) {
                throw new IllegalArgumentException("line amounts must not be negative");
            }
            reference.ifPresent(r -> IdRules.requireToken("reference", r));
            batchId.ifPresent(b -> IdRules.requireToken("batchId", b));
        }

        /** A line that is neither INFERRED nor AMBIGUOUS, so it has no candidate hash and no witnesses. */
        public Line(MerchantId merchantId, String fileId, int lineNo, LocalDate settlementDate, LocalDate valueDate,
                    Money amount, Optional<Money> fee, Optional<Money> tax, Optional<String> reference,
                    Optional<String> batchId, LineStatus status) {
            this(merchantId, fileId, lineNo, settlementDate, valueDate, amount, fee, tax, reference, batchId, status,
                    Optional.empty(), List.of());
        }

        /** {@code fileId:lineNo}, the subject key of a line's break and of its ledger causes. */
        public String key() {
            return fileId + ":" + lineNo;
        }

        /** The same line with another status that is neither INFERRED nor AMBIGUOUS. */
        public Line withStatus(LineStatus newStatus) {
            return new Line(merchantId, fileId, lineNo, settlementDate, valueDate, amount, fee, tax, reference, batchId,
                    newStatus);
        }

        /** The same line, MATCHED_INFERRED over the candidate set with this hash (§6.4). */
        public Line inferred(Sha256 hash) {
            return new Line(merchantId, fileId, lineNo, settlementDate, valueDate, amount, fee, tax, reference, batchId,
                    LineStatus.INFERRED, Optional.of(hash), List.of());
        }

        /** The same line, AMBIGUOUS: two distinct subsets of the candidates both sum to it (§6.4). */
        public Line ambiguous(List<PaymentId> first, List<PaymentId> second) {
            return new Line(merchantId, fileId, lineNo, settlementDate, valueDate, amount, fee, tax, reference, batchId,
                    LineStatus.AMBIGUOUS, Optional.empty(), List.of(first, second));
        }
    }

    /** Inserts or replaces the line {@code (merchant, fileId, lineNo)}. */
    void saveLine(Line line);

    Optional<Line> line(MerchantId merchantId, String fileId, int lineNo);

    /** The file's lines, by line number. */
    List<Line> lines(MerchantId merchantId, String fileId);

    /** The merchant's lines that reference {@code reference}, by file id, then line number. */
    List<Line> linesReferencing(MerchantId merchantId, String reference);

    /** A trailer the matcher has read, and whether its file is COMPLETE (§6.2). */
    record FileEnd(MerchantId merchantId, String fileId, LocalDate settlementDate, int lineCount, Money totalAmount,
                   Sha256 sha256, boolean complete) {

        public FileEnd {
            Objects.requireNonNull(merchantId, "merchantId");
            IdRules.requireToken("fileId", fileId);
            Objects.requireNonNull(settlementDate, "settlementDate");
            Objects.requireNonNull(totalAmount, "totalAmount");
            Objects.requireNonNull(sha256, "sha256");
            if (lineCount < 0) {
                throw new IllegalArgumentException("lineCount must not be negative");
            }
        }
    }

    /** Inserts or replaces the trailer of {@code (merchant, fileId)}. */
    void saveFileEnd(FileEnd end);

    Optional<FileEnd> fileEnd(MerchantId merchantId, String fileId);

    /** The COMPLETE file of the merchant for {@code settlementDate}, if any. */
    Optional<FileEnd> completeFile(MerchantId merchantId, LocalDate settlementDate);

    /** How a payment's settlement was matched (§6.3). */
    enum MatchHow {
        REFERENCED,
        SHORT_EXPLAINED,
        SHORT_PARTIAL,
        OVER_HELD,
        /** An unreferenced line's unique subset-sum solution (§6.4). */
        INFERRED
    }

    record Match(MerchantId merchantId, PaymentId paymentId, String fileId, int lineNo, MatchHow how) {

        public Match {
            Objects.requireNonNull(merchantId, "merchantId");
            Objects.requireNonNull(paymentId, "paymentId");
            IdRules.requireToken("fileId", fileId);
            Objects.requireNonNull(how, "how");
            if (lineNo < 1) {
                throw new IllegalArgumentException("line numbers start at 1");
            }
        }
    }

    /**
     * Records the payment's settlement match, unless the payment is already matched (§6.3,
     * {@code UNIQUE(merchant_id, payment_id)}).
     *
     * @return empty if this match was recorded; otherwise the existing match, unchanged
     */
    Optional<Match> match(Match match);

    Optional<Match> matchOf(MerchantId merchantId, PaymentId paymentId);
}
