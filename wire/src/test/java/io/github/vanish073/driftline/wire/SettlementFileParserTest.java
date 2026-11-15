package io.github.vanish073.driftline.wire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.settlement.MalformedSettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFile;
import io.github.vanish073.driftline.wire.settlement.SettlementFileParser;
import io.github.vanish073.driftline.wire.settlement.SettlementFileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Driftline Settlement File v1 (DESIGN §6.1, docs/formats.md): golden files, every rejection rule, round trips. */
class SettlementFileParserTest {

    private static final MerchantId M0 = MerchantId.of("m_0");
    private static final MerchantId M1 = MerchantId.of("m_1");

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = SettlementFileParserTest.class.getResourceAsStream("/settlement/" + name)) {
            return in.readAllBytes();
        }
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- golden files -----------------------------------------------------------------------------------------

    @Test
    void theDocumentedExampleParsesFieldByField() throws IOException {
        SettlementFile f = SettlementFileParser.parse(M0, resource("golden-two-lines.csv"));

        assertThat(f.fileId()).isEqualTo("stl_m_0_20261114");
        assertThat(f.merchantId()).isEqualTo(M0);
        assertThat(f.settlementDate()).isEqualTo(LocalDate.parse("2026-11-14"));
        assertThat(f.generatedAt()).isEqualTo(Instant.parse("2026-11-14T04:30:00Z"));
        assertThat(f.totalAmount()).isEqualTo(Money.ofPaise(674_536));
        assertThat(f.lines()).hasSize(2);

        SettlementLinePayload itemised = f.lines().get(0);
        assertThat(itemised.lineNo()).isEqualTo(1);
        assertThat(itemised.amount()).isEqualTo(Money.ofPaise(478_436));
        assertThat(itemised.fee()).contains(Money.ofPaise(9_800));
        assertThat(itemised.tax()).contains(Money.ofPaise(1_764));
        assertThat(itemised.reference()).contains("pay_12_a");
        assertThat(itemised.batchId()).contains("UTR20261114M0");
        assertThat(itemised.narration()).isEqualTo("settlement pay_12_a, T+1");
        assertThat(itemised.amount().plus(itemised.fee().get()).plus(itemised.tax().get()))
                .as("net + fee + tax = the gross of a 4900.00 payment").isEqualTo(Money.ofPaise(490_000));

        SettlementLinePayload plain = f.lines().get(1);
        assertThat(plain.fee()).isEmpty();
        assertThat(plain.tax()).isEmpty();
        assertThat(plain.settlementDate()).isEqualTo(f.settlementDate());
        assertThat(plain.fileId()).isEqualTo(f.fileId());
    }

    @Test
    void anEmptyDayIsAValidFileWithZeroLines() throws IOException {
        SettlementFile f = SettlementFileParser.parse(M1, resource("golden-empty-day.csv"));
        assertThat(f.lines()).isEmpty();
        assertThat(f.totalAmount()).isEqualTo(Money.ZERO);
        assertThat(f.generatedAt()).isEqualTo(Instant.parse("2026-11-15T04:30:00.000123Z"));
    }

    @Test
    void crlfBreaksQuotedQuotesAndANearOverflowTotalParseExactly() throws IOException {
        SettlementFile f = SettlementFileParser.parse(M0, resource("golden-crlf-no-final-break.csv"));
        assertThat(f.lines().get(0).narration()).isEqualTo("refund \"netted\" here");
        assertThat(f.lines().get(0).amount()).isEqualTo(Money.ofPaise(5));
        assertThat(f.lines().get(0).reference()).isEmpty();
        assertThat(f.lines().get(1).batchId()).contains("BATCH-7");
        assertThat(f.totalAmount()).isEqualTo(Money.ofPaise(9_223_372_036_854_775_805L));
    }

    @Test
    void writingAParsedCanonicalFileGivesBackTheSameBytes() throws IOException {
        for (String name : List.of("golden-two-lines.csv", "golden-empty-day.csv")) {
            byte[] raw = resource(name);
            MerchantId m = name.contains("empty") ? M1 : M0;
            assertThat(new String(SettlementFileWriter.write(SettlementFileParser.parse(m, raw)), StandardCharsets.UTF_8))
                    .as(name).isEqualTo(new String(raw, StandardCharsets.UTF_8));
        }
    }

    @Test
    void lineEndingsAndAFinalBreakDoNotChangeTheResult() throws IOException {
        byte[] lf = resource("golden-two-lines.csv");
        String text = new String(lf, StandardCharsets.UTF_8);
        SettlementFile expected = SettlementFileParser.parse(M0, lf);
        assertThat(SettlementFileParser.parse(M0, utf8(text.replace("\n", "\r\n")))).isEqualTo(expected);
        assertThat(SettlementFileParser.parse(M0, utf8(text.substring(0, text.length() - 1)))).isEqualTo(expected);
    }

    // ---- rejections -------------------------------------------------------------------------------------------

    private static final String H = "H,DLSF,1,stl_1,m_0,2026-11-14,2026-11-14T04:30:00Z";
    private static final String D1 = "D,1,2026-11-14,100.00,2.00,0.36,pay_1,UTR1,first";
    private static final String D2 = "D,2,2026-11-14,50.50,,,pay_2,,second";
    private static final String T = "T,2,150.50";

    private static String file(String... records) {
        return String.join("\n", records) + "\n";
    }

    static Stream<Arguments> malformed() {
        return Stream.of(
                // structure
                Arguments.of("empty file", "", 0, "empty"),
                Arguments.of("no header", file(D1, D2, T), 1, "header"),
                Arguments.of("no trailer", file(H, D1, D2), 3, "no trailer"),
                Arguments.of("trailer not last", file(H, D1, T, D2), 3, "last record"),
                Arguments.of("second header", file(H, H, D1, D2, T), 2, "second header"),
                Arguments.of("unknown record type", file(H, D1, "X,1,2", D2, T), 3, "unknown record type"),
                Arguments.of("lower-case record type", file(H, D1, D2, "t,2,150.50"), 4, "unknown record type"),
                Arguments.of("blank line", H + "\n\n" + D1 + "\n" + D2 + "\n" + T + "\n", 2, "blank line"),
                Arguments.of("two final breaks", file(H, D1, D2, T) + "\n", 5, "blank line"),
                Arguments.of("header field count", file("H,DLSF,1,stl_1,m_0,2026-11-14", D1, D2, T), 1, "7 fields"),
                Arguments.of("detail field count", file(H, D1, "D,2,2026-11-14,50.50,,,pay_2,second", T), 3, "9 fields"),
                Arguments.of("detail with an extra comma", file(H, D1, D2 + ",", T), 3, "9 fields"),
                Arguments.of("trailer field count", file(H, D1, D2, "T,2"), 4, "3 fields"),
                Arguments.of("wrong format name", file(H.replace("DLSF", "CSV"), D1, D2, T), 1, "format"),
                Arguments.of("wrong version", file(H.replace("DLSF,1", "DLSF,2"), D1, D2, T), 1, "format"),
                Arguments.of("another merchant's file", file(H.replace("m_0", "m_9"), D1, D2, T), 1, "uploaded for"),
                // trailer arithmetic
                Arguments.of("line count too high", file(H, D1, D2, "T,3,150.50"), 4, "line_count"),
                Arguments.of("line count too low", file(H, D1, D2, "T,1,150.50"), 4, "line_count"),
                Arguments.of("total off by one paisa", file(H, D1, D2, "T,2,150.49"), 4, "total_amount"),
                Arguments.of("total ignores a fee", file(H, D1, D2, "T,2,152.86"), 4, "total_amount"),
                Arguments.of("overflowing running total",
                        file(H, "D,1,2026-11-14,92233720368547758.07,,,,,a", "D,2,2026-11-14,0.01,,,,,b",
                                "T,2,0.00"), 3, "overflows"),
                // line numbers
                Arguments.of("line number gap", file(H, D1, D2.replace("D,2,", "D,3,"), T), 3, "line_no must be 2"),
                Arguments.of("line number repeat", file(H, D1, D1, T), 3, "line_no must be 2"),
                Arguments.of("line number with leading zero", file(H, D1.replace("D,1,", "D,01,"), D2, T), 2, "leading zero"),
                // amounts
                Arguments.of("three decimal places", file(H, D1.replace("100.00", "100.000"), D2, T), 2, "two decimals"),
                Arguments.of("one decimal place", file(H, D1.replace("100.00", "100.0"), D2, T), 2, "two decimals"),
                Arguments.of("no decimals", file(H, D1.replace("100.00", "100"), D2, T), 2, "two decimals"),
                Arguments.of("leading zero", file(H, D1.replace("100.00", "0100.00"), D2, T), 2, "two decimals"),
                Arguments.of("minus sign", file(H, D1.replace("100.00", "-100.00"), D2, T), 2, "two decimals"),
                Arguments.of("plus sign", file(H, D1.replace("100.00", "+100.00"), D2, T), 2, "two decimals"),
                Arguments.of("exponent", file(H, D1.replace("100.00", "1e2"), D2, T), 2, "two decimals"),
                Arguments.of("grouping", file(H, D2.replace("D,2,", "D,1,").replace("50.50", "\"1,050.50\""),
                        "T,1,1050.50"), 2, "two decimals"),
                Arguments.of("space in an amount", file(H, D1.replace("100.00", " 100.00"), D2, T), 2, "two decimals"),
                Arguments.of("amount too large", file(H, "D,1,2026-11-14,92233720368547758.08,,,,,a",
                        "T,1,92233720368547758.08"), 2, "too large"),
                Arguments.of("bad fee", file(H, D1.replace("2.00", "2"), D2, T), 2, "fee"),
                Arguments.of("bad tax", file(H, D1.replace("0.36", ".36"), D2, T), 2, "tax"),
                Arguments.of("bad total", file(H, D1, D2, "T,2,150.5"), 4, "total_amount"),
                // dates, ids, text
                Arguments.of("impossible date", file(H.replace("2026-11-14,2026", "2026-02-30,2026"), D1, D2, T), 1,
                        "calendar date"),
                Arguments.of("date with slashes", file(H, D1.replace("D,1,2026-11-14", "D,1,2026/11/14"), D2, T), 2,
                        "YYYY-MM-DD"),
                Arguments.of("local generated_at", file(H.replace("04:30:00Z", "04:30:00"), D1, D2, T), 1, "generated_at"),
                Arguments.of("nanosecond generated_at", file(H.replace("04:30:00Z", "04:30:00.000000001Z"), D1, D2, T), 1,
                        "generated_at"),
                Arguments.of("reference with a space", file(H, D1.replace("pay_1", "\"pay 1\""), D2, T), 2, "reference"),
                Arguments.of("file id too long", file(H.replace("stl_1", "f".repeat(65)), D1, D2, T), 1, "file_id"),
                Arguments.of("narration too long", file(H, D1.replace("first", "n".repeat(141)), D2, T), 2, "narration"),
                Arguments.of("tab character", file(H, D1.replace("first", "fi\trst"), D2, T), 2, "control character"),
                Arguments.of("C1 control character", file(H, D1.replace("first", "fi\u0085rst"), D2, T), 2,
                        "control character"),
                Arguments.of("unclosed quote", file(H, D1.replace("first", "\"first"), D2, T), 2, "not closed"),
                Arguments.of("text after a closing quote", file(H, D1.replace("first", "\"fir\"st"), D2, T), 2,
                        "closing quote"),
                Arguments.of("quote in an unquoted field", file(H, D1.replace("first", "fi\"rst"), D2, T), 2,
                        "unquoted field"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformed")
    void isRejectedWithTheRecordAndTheRule(String name, String text, int record, String reason) {
        assertThatThrownBy(() -> SettlementFileParser.parse(M0, utf8(text)))
                .isInstanceOf(MalformedSettlementFile.class)
                .hasMessageContaining(reason)
                .extracting(e -> ((MalformedSettlementFile) e).record()).isEqualTo(record);
    }

    @Test
    void theBaseOfTheRejectionCasesIsItselfValid() {
        SettlementFile f = SettlementFileParser.parse(M0, utf8(file(H, D1, D2, T)));
        assertThat(f.lines()).hasSize(2);
        assertThat(f.totalAmount()).isEqualTo(Money.ofPaise(15_050));
    }

    @Test
    void aByteOrderMarkIsRejected() {
        byte[] withBom = utf8("﻿" + file(H, D1, D2, T));
        assertThatThrownBy(() -> SettlementFileParser.parse(M0, withBom))
                .isInstanceOf(MalformedSettlementFile.class).hasMessageContaining("byte-order mark");
    }

    @Test
    void invalidUtf8IsRejected() {
        byte[] raw = utf8(file(H, D1, D2, T));
        byte[] broken = new byte[raw.length + 1];
        System.arraycopy(raw, 0, broken, 0, raw.length - 1);
        broken[raw.length - 1] = (byte) 0xC3;   // a lead byte with no continuation
        broken[raw.length] = '\n';
        assertThatThrownBy(() -> SettlementFileParser.parse(M0, broken))
                .isInstanceOf(MalformedSettlementFile.class).hasMessageContaining("UTF-8");
    }

    @Test
    void messagesNeverEchoTheRejectedValue() {
        String secret = "pay 4111111111111111";
        assertThatThrownBy(() -> SettlementFileParser.parse(M0, utf8(file(H, D1.replace("pay_1", "\"" + secret + "\""), D2, T))))
                .isInstanceOf(MalformedSettlementFile.class)
                .message().doesNotContain("4111");
    }

    // ---- round trip -------------------------------------------------------------------------------------------

    @Property(tries = 500)
    void anythingTheWriterWritesParsesBackToTheSameFile(@ForAll("files") SettlementFile file) {
        assertThat(SettlementFileParser.parse(file.merchantId(), SettlementFileWriter.write(file))).isEqualTo(file);
    }

    @Provide
    Arbitrary<SettlementFile> files() {
        Arbitrary<Long> paise = Arbitraries.oneOf(Arbitraries.longs().between(0, 10_000_000L),
                Arbitraries.longs().between(0, 100_000_000_000_000_000L));
        Arbitrary<Optional<Money>> optionalMoney = paise.map(Money::ofPaise).optional(0.5);
        Arbitrary<Optional<String>> token = Arbitraries.strings().withChars("abcXYZ019._-").ofMinLength(1)
                .ofMaxLength(64).optional(0.5);
        Arbitrary<String> narration = Arbitraries.strings().withChars("ab ,\"é€x1").ofMaxLength(140);
        Arbitrary<List<Object[]>> rows = Combinators.combine(paise, optionalMoney, optionalMoney, token, token, narration)
                .as((a, fee, tax, ref, batch, text) -> new Object[] {a, fee, tax, ref, batch, text})
                .list().ofMaxSize(20);
        return Combinators.combine(rows, Arbitraries.integers().between(0, 400), Arbitraries.longs().between(0, 999_999))
                .as((rs, day, micros) -> {
                    LocalDate date = LocalDate.parse("2026-01-01").plusDays(day);
                    List<SettlementLinePayload> lines = new ArrayList<>();
                    for (Object[] r : rs) {
                        @SuppressWarnings("unchecked")
                        SettlementLinePayload line = new SettlementLinePayload("stl_x", lines.size() + 1, date,
                                date.minusDays(lines.size() % 3), Money.ofPaise((Long) r[0]),
                                (Optional<Money>) r[1], (Optional<Money>) r[2], (Optional<String>) r[3],
                                (Optional<String>) r[4], (String) r[5]);
                        lines.add(line);
                    }
                    return SettlementFile.of("stl_x", M0, date, Instant.parse("2026-01-01T00:00:00Z")
                            .plusSeconds(day * 86_400L).plusNanos(micros * 1_000), lines);
                });
    }
}
