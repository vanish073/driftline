package io.github.vanish073.driftline.sandbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import io.github.vanish073.driftline.sim.world.Scenario;
import io.github.vanish073.driftline.wire.hmac.WebhookSignature;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The injector plays the simulator's scenarios against an HTTP endpoint (D51): webhooks arrive re-signed with the real
 * clock, order events carry the merchant's token and a key, settlement files arrive for the past days, and the
 * simulator's Oracle names the breaks the world implies.
 */
class InjectionTest {

    static final LocalDate TODAY = LocalDate.parse("2026-11-15");

    HttpServer server;
    BootstrapFile.Pool pool;
    final AtomicInteger badSignatures = new AtomicInteger();
    final AtomicInteger badTokens = new AtomicInteger();
    final AtomicInteger files = new AtomicInteger();

    @BeforeEach
    void start() throws IOException {
        pool = BootstrapFile.read(BootstrapFile.generate(3, 12, TODAY.minusDays(3), new SecureRandom()));
        BootstrapFile.Merchant m = pool.merchants().get(0);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            if (path.startsWith("/v1/webhooks/gateway/")) {
                WebhookSignature.Verification v = WebhookSignature.verify(exchange.getRequestHeaders()
                        .getFirst(WebhookSignature.HEADER), body, List.of(m.secret().getBytes(StandardCharsets.US_ASCII)),
                        Instant.now());
                if (!v.valid()) {
                    badSignatures.incrementAndGet();
                }
            } else if (!("Bearer " + m.token()).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                badTokens.incrementAndGet();
            } else if (path.endsWith("/settlement-files")) {
                files.incrementAndGet();
            }
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private Injection.Report inject(Scenario scenario, int count) {
        return Injection.run(pool.merchants().get(0), scenario, count,
                new HttpDriftline(URI.create("http://127.0.0.1:" + server.getAddress().getPort()), pool.merchants()),
                TODAY);
    }

    @Test
    void aDuplicateCaptureInjectionExpectsOneBreakPerExtraPaymentAndSignsWithTheRealClock() {
        Injection.Report r = inject(Scenario.DUPLICATE_CAPTURE, 3);
        assertThat(r.expected()).hasSize(3).allMatch(k -> k.startsWith("DUPLICATE_CAPTURE m_e2e_01/pay_"));
        assertThat(r.webhooks()).isGreaterThanOrEqualTo(12);   // pending and succeeded, two payments, three orders
        assertThat(r.orderEvents()).isGreaterThanOrEqualTo(9);
        assertThat(r.files()).isEqualTo(Injection.FILE_DAYS);
        assertThat(r.notAccepted()).isZero();
        assertThat(badSignatures).hasValue(0);
        assertThat(badTokens).hasValue(0);
        assertThat(files).hasValue(Injection.FILE_DAYS);
    }

    @Test
    void eachOneToOneScenarioExpectsItsOwnBreakType() {
        for (Scenario s : Injection.oneToOne()) {
            Injection.Report r = inject(s, 2);
            assertThat(r.expected()).as(s.name()).anyMatch(k -> k.startsWith(s.name() + " "));
        }
    }

    @Test
    void theInjectedWorldNeverRunsAheadOfTheRealClock() {
        assertThatThrownBy(() -> Injection.run(pool.merchants().get(0), Scenario.NORMAL, 1,
                new HttpDriftline(URI.create("http://127.0.0.1:1"), pool.merchants()), TODAY.minusDays(2)))
                .hasMessageContaining("days before today");
    }
}
