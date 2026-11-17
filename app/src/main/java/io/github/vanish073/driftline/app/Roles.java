package io.github.vanish073.driftline.app;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * {@code DRIFTLINE_ROLE} → Spring profiles (DESIGN §3.3, D49). One image runs any role; {@code all} (the laptop default)
 * runs ingest, the matcher, the settlement worker, the resolver and the dispatcher in one JVM, and a comma-separated
 * list of those runs just them ({@code scripts/chaos.sh} runs the matcher and the dispatcher in containers of their
 * own, so it can kill them). {@code migrate} is the one-shot Flyway service of the Compose stack (§15.2) and
 * {@code verify} the one-shot post-run check ({@code driftline verify}); each runs alone.
 */
public final class Roles {

    public static final String INGEST = "ingest";
    public static final String MATCHER = "matcher";
    public static final String SETTLEMENT_WORKER = "settlement-worker";
    public static final String RESOLVER = "resolver";
    public static final String DISPATCHER = "dispatcher";
    public static final String MIGRATE = "migrate";
    public static final String VERIFY = "verify";
    /** A Spring profile expression: every service role, not a one-shot. */
    public static final String SERVICE = "!" + MIGRATE + " & !" + VERIFY;

    private static final List<String> SERVICES = List.of(INGEST, MATCHER, SETTLEMENT_WORKER, RESOLVER, DISPATCHER);

    private Roles() {
    }

    public static List<String> profiles(String role) {
        String r = role.trim().toLowerCase(Locale.ROOT);
        switch (r) {
            case "all" -> {
                return SERVICES;
            }
            case MIGRATE, VERIFY -> {
                return List.of(r);
            }
            default -> {
                List<String> out = new ArrayList<>();
                for (String part : r.split(",", -1)) {
                    String p = part.trim();
                    if (!SERVICES.contains(p)) {
                        throw new IllegalArgumentException("DRIFTLINE_ROLE must be all, migrate, verify, or one or more "
                                + "of ingest, matcher, settlement-worker, resolver and dispatcher, comma-separated");
                    }
                    if (!out.contains(p)) {
                        out.add(p);
                    }
                }
                return List.copyOf(out);
            }
        }
    }
}
