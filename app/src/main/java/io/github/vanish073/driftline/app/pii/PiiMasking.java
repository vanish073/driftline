package io.github.vanish073.driftline.app.pii;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks personal data in text (DESIGN §14, D77): emails and UPI VPAs ({@code ab***@okaxis}) and Indian mobile numbers
 * ({@code 98******21}). Applied to every log message and stack trace by the JSON log encoder
 * ({@link io.github.vanish073.driftline.app.observability.JsonLogEncoder}, D78), and to contact details the ops API
 * shows. It is a net under the rule that personal
 * data is never logged, not a licence to log it: it masks what looks like a contact, so it can miss an unusual one and
 * may mask a harmless string that looks like one.
 */
public final class PiiMasking {

    /** local@domain, where the domain starts with a letter (so most {@code Object@1a2b3c} hashes are left alone). */
    private static final Pattern AT = Pattern.compile(
            "(?<![A-Za-z0-9._%+-])([A-Za-z0-9._%+-]{1,64})@([A-Za-z][A-Za-z0-9-]*(?:\\.[A-Za-z0-9-]+)*)");
    /** A ten-digit Indian mobile, optionally after +91 or 91, not inside a longer number. */
    private static final Pattern PHONE = Pattern.compile("(?<![A-Za-z0-9_])((?:\\+?91[ -]?)?)([6-9][0-9]{9})(?![A-Za-z0-9_])");

    private PiiMasking() {
    }

    public static String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String out = replace(AT, text, m -> keep(m.group(1)) + "***@" + m.group(2));
        return replace(PHONE, out, m -> m.group(1) + maskPhone(m.group(2)));
    }

    /** {@code 98******21}. */
    public static String maskPhone(String tenDigits) {
        return tenDigits.substring(0, 2) + "******" + tenDigits.substring(tenDigits.length() - 2);
    }

    /** {@code ab***@okaxis}, for an email or a VPA. */
    public static String maskAddress(String address) {
        int at = address.lastIndexOf('@');
        return at < 0 ? "***" : keep(address.substring(0, at)) + "***@" + address.substring(at + 1);
    }

    private static String keep(String local) {
        return local.length() <= 2 ? local.substring(0, 1) : local.substring(0, 2);
    }

    private interface Replacement {
        String of(Matcher m);
    }

    private static String replace(Pattern p, String text, Replacement r) {
        Matcher m = p.matcher(text);
        StringBuilder out = new StringBuilder(text.length());
        while (m.find()) {
            m.appendReplacement(out, Matcher.quoteReplacement(r.of(m)));
        }
        m.appendTail(out);
        return out.toString();
    }
}
