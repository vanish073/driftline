package io.github.vanish073.driftline.app;

import java.util.List;
import java.util.Locale;

/**
 * {@code DRIFTLINE_ROLE} → Spring profiles (DESIGN §3.3, D49). One image runs any role; {@code all} (the laptop default)
 * runs ingest, the matcher and the settlement worker in one JVM. {@code migrate} is the one-shot Flyway service of the
 * Compose stack (§15.2). The resolver and dispatcher roles are refused until they exist (Shift 8), so a deployment can
 * never believe it is running something that is not there.
 */
public final class Roles {

    public static final String INGEST = "ingest";
    public static final String MATCHER = "matcher";
    public static final String SETTLEMENT_WORKER = "settlement-worker";
    public static final String MIGRATE = "migrate";

    private Roles() {
    }

    public static List<String> profiles(String role) {
        return switch (role.trim().toLowerCase(Locale.ROOT)) {
            case "all" -> List.of(INGEST, MATCHER, SETTLEMENT_WORKER);
            case INGEST -> List.of(INGEST);
            case MATCHER -> List.of(MATCHER);
            case SETTLEMENT_WORKER -> List.of(SETTLEMENT_WORKER);
            case MIGRATE -> List.of(MIGRATE);
            case "resolver", "dispatcher" -> throw new IllegalArgumentException(
                    "DRIFTLINE_ROLE=" + role + " arrives with the resolver and dispatcher in Shift 8");
            default -> throw new IllegalArgumentException("DRIFTLINE_ROLE must be one of all, ingest, matcher, "
                    + "settlement-worker or migrate");
        };
    }
}
