package io.github.vanish073.driftline.wire.settlement;

import io.github.vanish073.driftline.core.event.SettlementLinePayload;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.wire.WireTime;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Writes Driftline Settlement File v1 in its canonical form: LF line breaks, a final line break, a field quoted only
 * when it holds a comma or a quote. The simulator's bank uses it, so the real parser reads what the world writes, and
 * tests use it to build files. Anything written here parses back to an equal {@link SettlementFile}.
 */
public final class SettlementFileWriter {

    private SettlementFileWriter() {
    }

    public static byte[] write(SettlementFile file) {
        StringBuilder out = new StringBuilder();
        out.append("H,").append(SettlementFileParser.FORMAT).append(',').append(SettlementFileParser.VERSION)
                .append(',').append(file.fileId())
                .append(',').append(file.merchantId().value())
                .append(',').append(file.settlementDate())
                .append(',').append(WireTime.format(file.generatedAt()))
                .append('\n');
        for (SettlementLinePayload line : file.lines()) {
            out.append("D,").append(line.lineNo())
                    .append(',').append(line.valueDate())
                    .append(',').append(line.amount())
                    .append(',').append(optionalMoney(line.fee()))
                    .append(',').append(optionalMoney(line.tax()))
                    .append(',').append(line.reference().orElse(""))
                    .append(',').append(line.batchId().orElse(""))
                    .append(',').append(narration(line))
                    .append('\n');
        }
        out.append("T,").append(file.lines().size()).append(',').append(file.totalAmount()).append('\n');
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String optionalMoney(Optional<Money> money) {
        return money.map(Money::toString).orElse("");
    }

    private static String narration(SettlementLinePayload line) {
        String text = line.narration();
        if (text.codePointCount(0, text.length()) > SettlementFileParser.MAX_NARRATION) {
            throw new IllegalArgumentException("line " + line.lineNo() + ": narration is too long");
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < 0x20 || c == 0x7F || (c >= 0x80 && c < 0xA0)) {
                throw new IllegalArgumentException("line " + line.lineNo() + ": narration has a control character");
            }
        }
        if (text.indexOf(',') < 0 && text.indexOf('"') < 0) {
            return text;
        }
        return '"' + text.replace("\"", "\"\"") + '"';
    }
}
