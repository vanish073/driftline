package io.github.vanish073.driftline.app.observability;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Ingest's metrics (DESIGN §13): {@code driftline_ingest_requests_total{route,status}} for every request to an ingest
 * route, counted by {@link #filter()} ahead of security so refusals are counted too, and
 * {@code driftline_webhook_rejected_total{reason}} with a bounded reason. Paths and bodies never become labels.
 */
public final class IngestMetrics {

    private static final Pattern WEBHOOK = Pattern.compile("/v1/webhooks/gateway/[^/]+");
    private static final Pattern ORDER_EVENTS = Pattern.compile("/v1/merchants/[^/]+/order-events");
    private static final Pattern SETTLEMENT_FILES = Pattern.compile("/v1/merchants/[^/]+/settlement-files");

    private final MeterRegistry registry;

    public IngestMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** The ingest route of a path, if it is one. */
    static Optional<String> route(String path) {
        if (WEBHOOK.matcher(path).matches()) {
            return Optional.of("webhook");
        }
        if (ORDER_EVENTS.matcher(path).matches()) {
            return Optional.of("order_event");
        }
        if (SETTLEMENT_FILES.matcher(path).matches()) {
            return Optional.of("settlement_file");
        }
        return Optional.empty();
    }

    /** The request-counting filter; register it ahead of Spring Security. */
    public OncePerRequestFilter filter() {
        return new OncePerRequestFilter() {
            @Override
            protected boolean shouldNotFilter(HttpServletRequest request) {
                return route(request.getRequestURI()).isEmpty();
            }

            @Override
            protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                            FilterChain chain) throws ServletException, IOException {
                String route = route(request.getRequestURI()).orElseThrow();
                try {
                    chain.doFilter(request, response);
                } finally {
                    registry.counter("driftline.ingest.requests", "route", route, "status",
                            Integer.toString(response.getStatus())).increment();
                }
            }
        };
    }

    /**
     * Counts a refused webhook. {@code reason} is a bounded label: {@code unknown_merchant}, {@code too_large},
     * {@code malformed}, or {@code signature_} and the signature failure.
     */
    public void webhookRejected(String reason) {
        registry.counter("driftline.webhook.rejected", "reason", reason).increment();
    }

    /** The label for a normaliser's rejection: its status and the reason text it gives (§10.2). */
    public static String rejectionReason(int status, String reason) {
        if (reason.startsWith("signature ")) {
            return "signature_" + reason.substring("signature ".length()).toLowerCase(Locale.ROOT);
        }
        return status == 413 ? "too_large" : "malformed";
    }
}
