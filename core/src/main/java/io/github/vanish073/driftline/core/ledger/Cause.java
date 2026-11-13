package io.github.vanish073.driftline.core.ledger;

import io.github.vanish073.driftline.core.id.IdRules;
import java.util.Objects;

/**
 * The economic event a ledger transaction records, such as {@code (CAPTURE, paymentId)} or
 * {@code (SETTLEMENT, fileId:lineNo)}. {@code UNIQUE(merchant, type, id)} is ledger layer 3 of DESIGN §9.3.
 */
public record Cause(CauseType type, String id) implements Comparable<Cause> {

    public Cause {
        Objects.requireNonNull(type, "type");
        IdRules.requireKey("causeId", id);
    }

    @Override
    public int compareTo(Cause other) {
        int byType = type.compareTo(other.type);
        return byType != 0 ? byType : id.compareTo(other.id);
    }

    @Override
    public String toString() {
        return type + ":" + id;
    }
}
