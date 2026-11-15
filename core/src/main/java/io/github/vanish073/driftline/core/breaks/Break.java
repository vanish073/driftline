package io.github.vanish073.driftline.core.breaks;

import io.github.vanish073.driftline.core.id.DerivedId;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * One finding (DESIGN §5.6, §12 {@code breaks}). A break is unique per {@code (merchant, type, subjectKey)}, and its
 * id is derived from exactly those (D4), so raising it twice, or replaying the batch that raised it, finds the same
 * row. {@code evidence} is a small sorted map of facts; late evidence is added to it and bumps {@code version}.
 *
 * @param subjectKey the payment id, {@code fileId:lineNo} or {@code date}, as §5.6 lists per type
 * @param detectedAt the processing time of the transaction that raised it (detection lag is measured from it)
 */
public record Break(String breakId, MerchantId merchantId, BreakType type, String subjectKey, BreakState state,
                    Instant detectedAt, SortedMap<String, String> evidence, long version) {

    /** Longest evidence value; evidence is a summary, never a document. */
    public static final int MAX_EVIDENCE_VALUE = 500;

    public Break {
        IdRules.requireToken("breakId", breakId);
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(type, "type");
        IdRules.requireKey("subjectKey", subjectKey);
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(detectedAt, "detectedAt");
        Objects.requireNonNull(evidence, "evidence");
        if (!breakId.equals(idOf(merchantId, type, subjectKey))) {
            throw new IllegalArgumentException("breakId is not derived from (merchant, type, subjectKey)");
        }
        if (version < 1) {
            throw new IllegalArgumentException("break versions start at 1, got " + version);
        }
        TreeMap<String, String> copy = new TreeMap<>();
        for (Map.Entry<String, String> e : evidence.entrySet()) {
            requireEvidence(e.getKey(), e.getValue());
            copy.put(e.getKey(), e.getValue());
        }
        evidence = Collections.unmodifiableSortedMap(copy);
    }

    public static String idOf(MerchantId merchantId, BreakType type, String subjectKey) {
        return "brk_" + DerivedId.of("break", merchantId.value(), type.name(), subjectKey).value();
    }

    /** A new break in {@link BreakState#DETECTED}, version 1. */
    public static Break detected(MerchantId merchantId, BreakType type, String subjectKey, Instant detectedAt,
                                 Map<String, String> evidence) {
        return new Break(idOf(merchantId, type, subjectKey), merchantId, type, subjectKey, BreakState.DETECTED,
                detectedAt, new TreeMap<>(evidence), 1);
    }

    public BreakKind kind() {
        return type.kind();
    }

    /** Evidence keys are tokens; values are 0 to 500 printable characters with no line breaks. */
    public static void requireEvidence(String key, String value) {
        IdRules.requireToken("evidence key", key);
        Objects.requireNonNull(value, "evidence value");
        if (value.length() > MAX_EVIDENCE_VALUE) {
            throw new IllegalArgumentException("evidence " + key + " is longer than " + MAX_EVIDENCE_VALUE);
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                throw new IllegalArgumentException("evidence " + key + " has a control character at index " + i);
            }
        }
    }
}
