package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.breaks.DeadlineRule;
import io.github.vanish073.driftline.core.id.IdRules;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.money.Money;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Breaks and their append-only transition log (DESIGN §8.1). Mirrors {@code breaks} and {@code break_transitions}
 * (§12). A break is unique per {@code (merchant, type, subjectKey)}.
 */
public interface BreakRepo {

    /** What {@link #raise} did, and the break as stored afterwards. */
    record Raised(boolean created, Break current) {
    }

    /**
     * Stores {@code b} with its first transition (no from-state, to {@code b.state()}), unless a break with the same
     * {@code (merchant, type, subjectKey)} exists; then nothing changes and the existing break is returned.
     */
    Raised raise(Break b, String reason);

    Optional<Break> find(MerchantId merchantId, BreakType type, String subjectKey);

    /** The break with this id, whatever its merchant (ids are derived from the merchant, D4); for the ops API. */
    Optional<Break> byId(String breakId);

    /**
     * Records one piece of evidence. An unchanged value changes nothing; a new key or a new value is stored and bumps
     * the version.
     *
     * @throws IllegalArgumentException if the break does not exist
     */
    void addEvidence(MerchantId merchantId, String breakId, String key, String value);

    /**
     * Moves the break to {@code to}, appends the transition and bumps the version.
     *
     * @throws IllegalArgumentException if the break does not exist, or is already in {@code to}
     */
    void transition(MerchantId merchantId, String breakId, BreakState to, Instant at, String reason);

    /** The merchant's breaks, ordered by type, then subject key. */
    List<Break> forMerchant(MerchantId merchantId);

    /** One break's transitions, oldest first. */
    List<BreakTransition> transitions(MerchantId merchantId, String breakId);

    // ---- the resolver's plan (§8.1, §8.3, §12; Shift 8) -----------------------------------------------------------

    /**
     * How the resolver handles a break: its deadline rule, deadline and {@code escalate_by}; when to look at it next;
     * the action it chose ({@code breaks.action}, set at most once); and, once closed, when, with the compensation
     * exposure at that moment. Instants are kept to the microsecond, as PostgreSQL stores them.
     */
    record Plan(DeadlineRule rule, Instant deadlineAt, Instant escalateBy, Instant reviewAt, Optional<String> action,
                Optional<Instant> resolvedAt, Money compensation) {
        public Plan {
            Objects.requireNonNull(rule, "rule");
            deadlineAt = Objects.requireNonNull(deadlineAt, "deadlineAt").truncatedTo(ChronoUnit.MICROS);
            escalateBy = Objects.requireNonNull(escalateBy, "escalateBy").truncatedTo(ChronoUnit.MICROS);
            reviewAt = Objects.requireNonNull(reviewAt, "reviewAt").truncatedTo(ChronoUnit.MICROS);
            Objects.requireNonNull(action, "action");
            action.ifPresent(a -> IdRules.requireToken("action", a));
            resolvedAt = Objects.requireNonNull(resolvedAt, "resolvedAt").map(t -> t.truncatedTo(ChronoUnit.MICROS));
            Objects.requireNonNull(compensation, "compensation");
            if (compensation.isNegative()) {
                throw new IllegalArgumentException("compensation must not be negative");
            }
            if (escalateBy.isAfter(deadlineAt)) {
                throw new IllegalArgumentException("escalate_by must not be after the deadline");
            }
        }

        public Plan withReviewAt(Instant at) {
            return new Plan(rule, deadlineAt, escalateBy, at, action, resolvedAt, compensation);
        }
    }

    /**
     * Stores the break's plan. Its action is set at most once: a plan that changes an action already stored is refused.
     *
     * @throws IllegalArgumentException if the break does not exist
     * @throws IllegalStateException    if the plan would change a stored action
     */
    void savePlan(MerchantId merchantId, String breakId, Plan plan);

    Optional<Plan> planOf(MerchantId merchantId, String breakId);

    /**
     * Breaks the resolver should look at: in DETECTED, AWAITING_EVIDENCE or ACTION_PENDING, and either not planned yet
     * or due ({@code reviewAt ≤ now}); unplanned first, then by {@code reviewAt}, then break id. PostgreSQL locks the rows
     * it returns with {@code FOR UPDATE SKIP LOCKED}, so two resolvers never take the same break at once.
     */
    List<Break> dueForReview(Instant now, int limit);

    /**
     * When the resolver next has work: the earliest {@code reviewAt} of a planned break in DETECTED, AWAITING_EVIDENCE
     * or ACTION_PENDING, or the earliest detection time of such a break not planned yet; empty if there is none. Drivers
     * sleep until then instead of polling.
     */
    Optional<Instant> nextReviewAt();

    /**
     * Asks the resolver to look at a planned break again no later than {@code at} (new evidence, a command result). An
     * unplanned break is already due, so nothing changes.
     */
    void wake(MerchantId merchantId, String breakId, Instant at);
}
