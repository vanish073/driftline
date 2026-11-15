package io.github.vanish073.driftline.wire.settlement;

import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.money.MoneyFormatException;
import io.github.vanish073.driftline.wire.WireTime;
import io.github.vanish073.driftline.wire.json.MalformedJson;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The strict parser for Driftline Settlement File v1 (DESIGN §6.1; the format and its sources are in
 * {@code docs/formats.md}). A pure function from bytes to a {@link SettlementFile}: all or nothing, so a file with
 * one bad record is rejected as a whole, with the record number and the rule that failed.
 *
 * <p>It checks structure and arithmetic only: one header first and one trailer last, fixed field counts, exact
 * two-decimal rupee amounts, consecutive line numbers, and a trailer whose count and total match the detail lines
 * exactly. Whether an amount is right for the payment it references is the matcher's call (§6.3, §6.5).
 */
public final class SettlementFileParser {

    public static final String FORMAT = "DLSF";
    public static final String VERSION = "1";
    public static final int MAX_NARRATION = 140;
    static final int HEADER_FIELDS = 7;
    static final int DETAIL_FIELDS = 9;
    static final int TRAILER_FIELDS = 3;

    private SettlementFileParser() {
    }

    /**
     * Parses {@code raw}, which was uploaded for {@code uploadedFor}.
     *
     * @throws MalformedSettlementFile if any rule of the format fails
     */
    public static SettlementFile parse(MerchantId uploadedFor, byte[] raw) {
        List<String> records = records(utf8(raw));
        if (records.isEmpty()) {
            throw new MalformedSettlementFile(0, "the file is empty");
        }

        List<String> h = fields(1, records.get(0));
        if (!h.get(0).equals("H")) {
            throw new MalformedSettlementFile(1, "the first record must be the header H");
        }
        requireCount(1, h, HEADER_FIELDS, "H");
        if (!h.get(1).equals(FORMAT) || !h.get(2).equals(VERSION)) {
            throw new MalformedSettlementFile(1, "the format must be " + FORMAT + " version " + VERSION);
        }
        String fileId = token(1, "file_id", h.get(3));
        MerchantId merchant = MerchantId.of(token(1, "merchant_id", h.get(4)));
        if (!merchant.equals(uploadedFor)) {
            throw new MalformedSettlementFile(1, "merchant_id is not the merchant the file was uploaded for");
        }
        LocalDate settlementDate = date(1, "settlement_date", h.get(5));
        Instant generatedAt = instant(1, h.get(6));

        List<SettlementLinePayload> lines = new ArrayList<>();
        Money sum = Money.ZERO;
        for (int i = 1; i < records.size(); i++) {
            int record = i + 1;
            List<String> f = fields(record, records.get(i));
            switch (f.get(0)) {
                case "D" -> {
                    requireCount(record, f, DETAIL_FIELDS, "D");
                    SettlementLinePayload line = detail(record, f, fileId, settlementDate, lines.size() + 1);
                    try {
                        sum = sum.plus(line.amount());
                    } catch (ArithmeticException e) {
                        throw new MalformedSettlementFile(record, "the running total of the amounts overflows");
                    }
                    lines.add(line);
                }
                case "T" -> {
                    if (record != records.size()) {
                        throw new MalformedSettlementFile(record, "the trailer T must be the last record");
                    }
                    requireCount(record, f, TRAILER_FIELDS, "T");
                    int lineCount = count(record, "line_count", f.get(1));
                    if (lineCount != lines.size()) {
                        throw new MalformedSettlementFile(record, "line_count does not match the " + lines.size()
                                + " detail records");
                    }
                    Money total = rupees(record, "total_amount", f.get(2));
                    if (!total.equals(sum)) {
                        throw new MalformedSettlementFile(record, "total_amount is not the exact sum of the amounts");
                    }
                    return new SettlementFile(fileId, merchant, settlementDate, generatedAt, lines, total);
                }
                case "H" -> throw new MalformedSettlementFile(record, "a second header H");
                default -> throw new MalformedSettlementFile(record, "unknown record type");
            }
        }
        throw new MalformedSettlementFile(records.size(), "the file has no trailer T");
    }

    private static SettlementLinePayload detail(int record, List<String> f, String fileId, LocalDate settlementDate,
                                                int expectedLineNo) {
        int lineNo = count(record, "line_no", f.get(1));
        if (lineNo != expectedLineNo) {
            throw new MalformedSettlementFile(record, "line_no must be " + expectedLineNo);
        }
        LocalDate valueDate = date(record, "value_date", f.get(2));
        Money amount = rupees(record, "amount", f.get(3));
        Optional<Money> fee = f.get(4).isEmpty() ? Optional.empty() : Optional.of(rupees(record, "fee", f.get(4)));
        Optional<Money> tax = f.get(5).isEmpty() ? Optional.empty() : Optional.of(rupees(record, "tax", f.get(5)));
        Optional<String> reference = f.get(6).isEmpty() ? Optional.empty() : Optional.of(token(record, "reference", f.get(6)));
        Optional<String> batchId = f.get(7).isEmpty() ? Optional.empty() : Optional.of(token(record, "batch_id", f.get(7)));
        String narration = narration(record, f.get(8));
        return new SettlementLinePayload(fileId, lineNo, settlementDate, valueDate, amount, fee, tax, reference, batchId,
                narration);
    }

    // ---- lexical layer --------------------------------------------------------------------------------------

    private static String utf8(byte[] raw) {
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(raw))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new MalformedSettlementFile(0, "the file is not valid UTF-8");
        }
        if (!text.isEmpty() && text.charAt(0) == '﻿') {
            throw new MalformedSettlementFile(0, "a byte-order mark is not allowed");
        }
        return text;
    }

    /** Splits on LF, drops one CR before each LF, allows one final line break, and rejects blank lines. */
    static List<String> records(String text) {
        List<String> out = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                out.add(text.substring(start, i > start && text.charAt(i - 1) == '\r' ? i - 1 : i));
                start = i + 1;
            }
        }
        if (start < text.length()) {
            out.add(text.substring(start));
        }
        for (int i = 0; i < out.size(); i++) {
            if (out.get(i).isEmpty()) {
                throw new MalformedSettlementFile(i + 1, "a blank line");
            }
        }
        return out;
    }

    /** RFC 4180 fields of one record. Quotes may wrap a field; {@code ""} inside one is a quote. */
    static List<String> fields(int record, String line) {
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                throw new MalformedSettlementFile(record, "a control character at column " + (i + 1));
            }
        }
        List<String> out = new ArrayList<>();
        int i = 0;
        int n = line.length();
        while (true) {
            StringBuilder field = new StringBuilder();
            if (i < n && line.charAt(i) == '"') {
                i++;
                while (true) {
                    if (i >= n) {
                        throw new MalformedSettlementFile(record, "a quoted field is not closed");
                    }
                    char c = line.charAt(i);
                    if (c == '"') {
                        if (i + 1 < n && line.charAt(i + 1) == '"') {
                            field.append('"');
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else {
                        field.append(c);
                        i++;
                    }
                }
                if (i < n && line.charAt(i) != ',') {
                    throw new MalformedSettlementFile(record, "text after a closing quote at column " + (i + 1));
                }
            } else {
                while (i < n && line.charAt(i) != ',') {
                    if (line.charAt(i) == '"') {
                        throw new MalformedSettlementFile(record, "a quote inside an unquoted field at column " + (i + 1));
                    }
                    field.append(line.charAt(i));
                    i++;
                }
            }
            out.add(field.toString());
            if (i >= n) {
                return out;
            }
            i++;
        }
    }

    // ---- field rules ----------------------------------------------------------------------------------------

    private static void requireCount(int record, List<String> f, int expected, String type) {
        if (f.size() != expected) {
            throw new MalformedSettlementFile(record, "a " + type + " record has " + expected + " fields, not " + f.size());
        }
    }

    private static String token(int record, String field, String value) {
        try {
            return IdRules.requireToken(field, value);
        } catch (IllegalArgumentException e) {
            throw new MalformedSettlementFile(record, e.getMessage());
        }
    }

    /** {@code 0} or a positive integer of at most 9 digits, with no sign and no leading zero. */
    private static int count(int record, String field, String value) {
        boolean digits = !value.isEmpty() && value.length() <= 9;
        for (int i = 0; digits && i < value.length(); i++) {
            digits = value.charAt(i) >= '0' && value.charAt(i) <= '9';
        }
        if (!digits || (value.length() > 1 && value.charAt(0) == '0')) {
            throw new MalformedSettlementFile(record, field + " must be a whole number without sign or leading zero");
        }
        return Integer.parseInt(value);
    }

    /** {@code YYYY-MM-DD}, a real calendar date. */
    private static LocalDate date(int record, String field, String value) {
        boolean shape = value.length() == 10 && value.charAt(4) == '-' && value.charAt(7) == '-';
        for (int i = 0; shape && i < 10; i++) {
            shape = i == 4 || i == 7 || (value.charAt(i) >= '0' && value.charAt(i) <= '9');
        }
        if (!shape) {
            throw new MalformedSettlementFile(record, field + " must be YYYY-MM-DD");
        }
        try {
            return LocalDate.parse(value);
        } catch (DateTimeException e) {
            throw new MalformedSettlementFile(record, field + " is not a calendar date");
        }
    }

    private static Instant instant(int record, String value) {
        try {
            return WireTime.parse("generated_at", value);
        } catch (MalformedJson e) {
            throw new MalformedSettlementFile(record, e.getMessage());
        }
    }

    /**
     * Rupees with exactly two decimals: {@code 0} or digits without a leading zero, a point, two digits. The shape is
     * checked here, because {@link Money#parse} also accepts one decimal; Money then converts exactly and rejects an
     * amount too large for 64-bit paise.
     */
    static Money rupees(int record, String field, String value) {
        int point = value.indexOf('.');
        boolean shape = point > 0 && point == value.length() - 3 && (point == 1 || value.charAt(0) != '0');
        for (int i = 0; shape && i < value.length(); i++) {
            shape = i == point || (value.charAt(i) >= '0' && value.charAt(i) <= '9');
        }
        if (!shape) {
            throw new MalformedSettlementFile(record, field + " must be rupees with exactly two decimals, such as 499.00");
        }
        try {
            return Money.parse(value);
        } catch (MoneyFormatException e) {
            throw new MalformedSettlementFile(record, field + " is too large");
        }
    }

    private static String narration(int record, String value) {
        if (value.codePointCount(0, value.length()) > MAX_NARRATION) {
            throw new MalformedSettlementFile(record, "narration is longer than " + MAX_NARRATION + " characters");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c >= 0x80 && c < 0xA0) {
                throw new MalformedSettlementFile(record, "narration has a control character at column " + (i + 1));
            }
        }
        return value;
    }
}
