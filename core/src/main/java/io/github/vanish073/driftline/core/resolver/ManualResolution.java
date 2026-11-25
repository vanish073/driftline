package io.github.vanish073.driftline.core.resolver;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakLifecycle;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import io.github.vanish073.driftline.core.port.AuditRepo;
import io.github.vanish073.driftline.core.port.BreakRepo.Plan;
import io.github.vanish073.driftline.core.port.Tx;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * {@code POST /v1/breaks/{id}/resolve} (DESIGN §8.1, §10.1, §12), sans-I/O: a person closes an ESCALATED break as
 * RESOLVED_MANUAL, in one transaction with its audit row (hash-chained, §7.6). Only ESCALATED → RESOLVED_MANUAL is in
 * the lifecycle (D60), so a break the resolver still owns cannot be closed by hand, and a closed break cannot be closed
 * again. The plan records when, and the compensation exposure at that moment (§8.3: escalation does not stop the
 * clock). Moving money by hand (MANUAL_ADJUSTMENT) is a separate, later API; this one only records the decision.
 */
public final class ManualResolution {

    public static final String ACTION = "BREAK_RESOLVE_MANUAL";
    private static final String REASON_PREFIX = "resolved by hand: ";
    /** The note becomes the transition's reason after a prefix, and a reason is at most 500 characters. */
    public static final int MAX_NOTE = Break.MAX_EVIDENCE_VALUE - REASON_PREFIX.length();

    /** What happened. */
    public sealed interface Result {
    }

    /** No such break, or not one this caller may see. */
    public record NotFound() implements Result {
    }

    /** The break is not ESCALATED. */
    public record NotEscalated(BreakState state) implements Result {
    }

    /** Resolved now, or, if {@code replayed}, by an earlier request with the same key and content (D94). */
    public record Resolved(String breakId, Instant at, Money compensation, long auditSeq, boolean replayed)
            implements Result {
    }

    /** The {@code Idempotency-Key} was used before, on this break, for a different request (D94). */
    public record KeyReused() implements Result {
    }

    private ManualResolution() {
    }

    /** @throws IllegalArgumentException if the note is missing, too long or has a control character */
    public static String requireNote(String note) {
        Objects.requireNonNull(note, "note");
        if (note.isBlank() || note.length() > MAX_NOTE) {
            throw new IllegalArgumentException("a note of 1 to " + MAX_NOTE + " characters is required");
        }
        Break.requireEvidence("note", note);
        return note;
    }

    /**
     * @param scope  the merchant the caller is bound to; empty for an ops token (every merchant)
     * @param actor  who resolved it, as the audit trail names them
     * @param note   why; required, at most {@link #MAX_NOTE} characters, no control characters
     * @param requestKey the caller's {@code Idempotency-Key}: a repeat answers as the first request did and changes
     *                   nothing, and the key with another note is refused (Shift 11, D94)
     * @throws IllegalArgumentException if the note is invalid ({@link #requireNote}), or if another caller closed the
     *                                  break between this read and this write (the row lock then sees it closed)
     */
    public static Result resolve(Tx tx, String breakId, Optional<MerchantId> scope, String actor, String note,
                                 String requestKey, Instant now) {
        requireNote(note);
        Optional<Break> found = tx.breaks().byId(breakId)
                .filter(b -> scope.map(m -> m.equals(b.merchantId())).orElse(true));
        if (found.isEmpty()) {
            return new NotFound();
        }
        Break b = found.get();
        io.github.vanish073.driftline.core.event.Sha256 request = ManualRequests.hash(ACTION, breakId, note);
        switch (ManualRequests.prior(tx.audit(), ACTION, breakId, requestKey, request)) {
            case ManualRequests.Different d -> {
                return new KeyReused();
            }
            case ManualRequests.Same same -> {
                AuditRepo.Row first = same.row();
                return new Resolved(breakId, first.entry().at(),
                        Money.parseSigned(first.entry().details().get("compensation")), first.seq(), true);
            }
            case ManualRequests.None none -> {
                // act below
            }
        }
        if (!BreakLifecycle.allowed(b.state(), BreakState.RESOLVED_MANUAL)) {
            return new NotEscalated(b.state());
        }
        MerchantId m = b.merchantId();
        tx.breaks().transition(m, breakId, BreakState.RESOLVED_MANUAL, now, REASON_PREFIX + note);
        Money compensation = Money.ZERO;
        Optional<Plan> plan = tx.breaks().planOf(m, breakId);
        if (plan.isPresent()) {
            Plan p = plan.get();
            compensation = p.rule().compensation(p.deadlineAt(), now);
            tx.breaks().savePlan(m, breakId, new Plan(p.rule(), p.deadlineAt(), p.escalateBy(), p.reviewAt(),
                    p.action(), Optional.of(now), compensation));
        }
        TreeMap<String, String> details = new TreeMap<>();
        details.put("merchant_id", m.value());
        details.put("type", b.type().name());
        details.put("subject", b.subjectKey());
        details.put("from", b.state().name());
        details.put("to", BreakState.RESOLVED_MANUAL.name());
        details.put("compensation", compensation.toString());
        details.put("note", note);
        ManualRequests.stamp(details, requestKey, request);
        AuditRepo.Row row = tx.audit().append(AuditRepo.Entry.of(now, actor, ACTION, breakId, details));
        // the stored instant (microseconds), so a replay's answer is the same as this one (D94)
        return new Resolved(breakId, row.entry().at(), compensation, row.seq(), false);
    }
}
