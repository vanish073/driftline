package io.github.vanish073.driftline.core.port;

import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakTransition;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.id.MerchantId;
import java.time.Instant;
import java.util.List;
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
}
