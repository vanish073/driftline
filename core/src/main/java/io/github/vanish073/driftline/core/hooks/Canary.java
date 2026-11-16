package io.github.vanish073.driftline.core.hooks;

/**
 * Bugs planted on purpose to measure whether the simulator catches them (DESIGN §11.10). Only the simulator and
 * tests may switch one on; production always runs with {@link Canaries#NONE}.
 */
public enum Canary {
    DEDUPE_OFF("C01", "skip the dedupe insert"),
    NO_REFUND_KEY("C02", "send refunds without an idempotency key"),
    MARK_BEFORE_SEND("C03", "mark the outbox command done before sending"),
    SKIP_TTL_LOOKUP("C04", "resend a refund without the lookup"),
    LEDGER_UNIQUE_OFF("C05", "post without the cause uniqueness check"),
    SUCCESS_TO_FAILED("C06", "allow SUCCEEDED to FAILED"),
    PROCESSING_TIME_WATERMARK("C07", "treat missing by wall clock"),
    FIRST_SOLUTION_WINS("C08", "subset-sum returns the first fit, with no ambiguity check"),
    NO_FENCING("C09", "commit without the epoch check"),
    IGNORE_ESCALATE_BY("C10", "never escalate on time"),
    BREAK_SCOPED_REFUND_KEY("C11", "refund key per break, and each break acts for itself (both halves of D11 off)");

    private final String code;
    private final String plantedBug;

    Canary(String code, String plantedBug) {
        this.code = code;
        this.plantedBug = plantedBug;
    }

    /** The canary with code {@code code}, such as {@code C09}. */
    public static Canary fromCode(String code) {
        for (Canary c : values()) {
            if (c.code.equals(code)) {
                return c;
            }
        }
        throw new IllegalArgumentException("no canary " + code);
    }

    public String code() {
        return code;
    }

    public String plantedBug() {
        return plantedBug;
    }
}
