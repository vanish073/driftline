package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.vanish073.driftline.app.dispatcher.CallCodecs;
import io.github.vanish073.driftline.app.dispatcher.DispatcherDriver;
import io.github.vanish073.driftline.app.port.HttpNetwork;
import io.github.vanish073.driftline.app.resolver.ResolverDriver;
import io.github.vanish073.driftline.core.breaks.Break;
import io.github.vanish073.driftline.core.breaks.BreakState;
import io.github.vanish073.driftline.core.breaks.BreakType;
import io.github.vanish073.driftline.core.dispatcher.Dispatcher;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoint;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.id.MerchantId;
import io.github.vanish073.driftline.core.port.CallRequest;
import io.github.vanish073.driftline.core.port.CallTarget;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.core.port.OutboundCall;
import io.github.vanish073.driftline.core.port.Store;
import io.github.vanish073.driftline.core.resolver.Resolver;
import io.github.vanish073.driftline.core.time.IstCalendar;
import io.github.vanish073.driftline.sandbox.BootstrapFile;
import io.github.vanish073.driftline.sandbox.SandboxServer;
import io.github.vanish073.driftline.wire.json.JsonObject;
import io.github.vanish073.driftline.wire.json.StrictJson;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Shift 8 Prove (DESIGN §9.4): each of the outbox's crash points CP1–CP5 strikes at the refund of a real break, against
 * real PostgreSQL and Kafka and the sandbox gateway over HTTP, and after a restart there is <b>exactly one refund</b> at
 * the gateway and <b>exactly one</b> REFUND ledger transaction.
 *
 * <p>The app runs ingest, the matcher and the settlement worker; the sandbox (in this JVM) injects one DEBITED_NO_ORDER
 * world into it and serves that world's gateway. The resolver and dispatcher are the production drivers, built here and
 * stepped by the test so each crash point is reached deterministically: an incarnation runs until its crash point
 * strikes (aimed at the refund by reading committed state), then a second incarnation, with a new lease owner and the
 * first one's in-flight replies lost, runs until the break is RESOLVED_AUTO. A crash is the crash point throwing, as in
 * the simulator: the transaction in progress rolls back and the incarnation is abandoned.
 *
 * <p>The dispatcher's call timeout is 3 s, so a lost reply is retried quickly; every retry of the refund looks it up
 * first (BUG-008).
 */
class OutboxCrashIT {

    static final String TOPIC = "outbox_crash_it.events";
    static final Dispatcher.Config DISPATCH = new Dispatcher.Config(Duration.ofSeconds(3), Duration.ofMillis(200),
            Duration.ofSeconds(1), 20);
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    static Path dir;
    static String url;
    static ConfigurableApplicationContext app;
    static SandboxServer sandbox;
    static BootstrapFile.Pool pool;
    static Store store;
    static Clock clock;
    static String api;
    static String sandboxUrl;

    @BeforeAll
    static void start() throws IOException {
        url = Containers.newDatabase("outbox_crash_it");
        Containers.createTopic(TOPIC, 12);
        byte[] bootstrap = BootstrapFile.generate(6, 12, IstCalendar.istDate(Instant.now()).minusDays(3),
                new SecureRandom());
        pool = BootstrapFile.read(bootstrap);
        Path file = dir.resolve("bootstrap.json");
        Files.write(file, bootstrap);
        SpringApplication s = new SpringApplication(DriftlineApplication.class);
        s.setAdditionalProfiles(Roles.INGEST, Roles.MATCHER, Roles.SETTLEMENT_WORKER);
        app = s.run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + Containers.APP_USER,
                "--spring.datasource.password=" + Containers.APP_PASSWORD,
                "--driftline.kafka.bootstrap-servers=" + Containers.KAFKA.getBootstrapServers(),
                "--driftline.kafka.topic=" + TOPIC,
                "--driftline.kafka.group-id=outbox-crash-it",
                "--driftline.kafka.session-timeout=6s",
                "--driftline.secrets.active-kid=it",
                "--driftline.secrets.keys.it=" + Base64.getEncoder().encodeToString(new byte[32]),
                "--driftline.bootstrap-file=" + file, "--driftline.environment=test",
                "--driftline.instance-id=outbox-crash",
                "--server.port=0",
                "--management.server.port=0");
        api = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
        store = app.getBean(Store.class);
        clock = app.getBean(Clock.class);
        sandbox = new SandboxServer(0, URI.create(api), pool);
        sandbox.start();
        sandboxUrl = "http://127.0.0.1:" + sandbox.port();
    }

    @AfterAll
    static void stop() {
        if (sandbox != null) {
            sandbox.stop();
        }
        if (app != null) {
            app.close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = CrashPoint.class, names = {"CP1", "CP2", "CP3", "CP4", "CP5"})
    void exactlyOneRefundAndOneRefundPostingAfterACrashAtTheRefund(CrashPoint point) throws Exception {
        JsonObject injected = StrictJson.parse(post(sandboxUrl + "/v1/inject?type=DEBITED_NO_ORDER&count=1"));
        MerchantId m = MerchantId.of(injected.string("merchant_id"));
        String expected = injected.string("expected_breaks");
        assertThat(expected).as("one DEBITED_NO_ORDER").startsWith("DEBITED_NO_ORDER ").doesNotContain(";");
        String payment = expected.substring(expected.indexOf('/') + 1);
        Containers.await("the DEBITED_NO_ORDER break for " + payment, Duration.ofSeconds(90),
                () -> store.transact("it", tx -> tx.breaks().find(m, BreakType.DEBITED_NO_ORDER, payment)).isPresent());
        String breakId = store.transact("it", tx -> tx.breaks().find(m, BreakType.DEBITED_NO_ORDER, payment))
                .orElseThrow().breakId();
        BooleanSupplier resolved = () -> store.transact("it", tx -> tx.breaks().byId(breakId))
                .map(Break::state).filter(BreakState.RESOLVED_AUTO::equals).isPresent();
        // The gateway's own refund webhook can close the break before the dispatcher's retry records its answer
        // (CP4, CP5), so the run goes on until the refund command is recorded too: §9.4's "retried with the same key".
        BooleanSupplier settled = () -> resolved.getAsBoolean() && Containers.count(url, "SELECT count(*) FROM outbox"
                + " WHERE break_id = '" + breakId + "' AND kind = 'REFUND' AND status = 'DONE'") == 1;

        Incarnation first = new Incarnation("outbox-crash-" + point + ".1", Optional.of(point), breakId);
        assertThat(first.run(resolved, Duration.ofSeconds(60))).as("%s struck at the refund", point).isTrue();
        assertThat(resolved.getAsBoolean()).as("the crash left the break unresolved").isFalse();

        Incarnation second = new Incarnation("outbox-crash-" + point + ".2", Optional.empty(), breakId);
        second.run(settled, Duration.ofSeconds(120));
        assertThat(resolved.getAsBoolean()).as("RESOLVED_AUTO after the restart").isTrue();

        JsonObject stats = StrictJson.parse(get(sandboxUrl + "/v1/stats?merchant=" + m.value()));
        assertThat(stats.integer("refunds")).as("refunds the gateway executed").isEqualTo(1);
        assertThat(stats.integer("payments_refunded_more_than_once")).isZero();
        assertThat(Containers.count(url, "SELECT count(*) FROM ledger_transactions WHERE merchant_id = '" + m.value()
                + "' AND cause_type = 'REFUND'")).as("REFUND ledger transactions").isEqualTo(1);
        assertThat(Containers.count(url, "SELECT count(*) FROM outbox WHERE break_id = '" + breakId
                + "' AND kind = 'REFUND'")).as("one refund command").isEqualTo(1);
        assertThat(Containers.count(url, "SELECT count(*) FROM outbox WHERE break_id = '" + breakId
                + "' AND kind = 'REFUND' AND status = 'DONE' AND result->>'status' = 'PROCESSED'"))
                .as("the refund command recorded as PROCESSED").isEqualTo(1);
    }

    /** One run of the resolver and dispatcher: a process, as far as leases and in-flight replies are concerned. */
    final class Incarnation {

        final LinkedBlockingQueue<HttpNetwork.CallOutcome> replies = new LinkedBlockingQueue<>();
        final ResolverDriver resolver;
        final DispatcherDriver dispatcher;
        final Optional<CrashPoint> armed;
        final String breakId;
        volatile boolean refundSent;
        volatile boolean refundJustSent;
        volatile boolean recordingRefund;

        Incarnation(String owner, Optional<CrashPoint> armed, String breakId) {
            this.armed = armed;
            this.breakId = breakId;
            CrashPoints crashPoints = this::reached;
            HttpNetwork network = new HttpNetwork(HTTP, Map.of(CallTarget.GATEWAY, URI.create(sandboxUrl + "/gateway"),
                    CallTarget.MERCHANT, URI.create(sandboxUrl + "/merchant"), CallTarget.OPS,
                    URI.create(sandboxUrl + "/ops"), CallTarget.DRIFTLINE, URI.create(api)),
                    CallCodecs.all(pool.serviceToken()), replies::add, Duration.ofSeconds(2));
            this.resolver = new ResolverDriver(store, new Resolver(Resolver.Config.DEFAULT, Canaries.NONE, crashPoints),
                    clock, crashPoints, Duration.ofMillis(100), 20, () -> { });
            this.dispatcher = new DispatcherDriver(store, new Dispatcher(DISPATCH, Canaries.NONE, crashPoints), clock,
                    crashPoints, inbox -> call -> {
                        boolean refund = call.request() instanceof CallRequest.Refund;
                        refundJustSent = refund;
                        refundSent |= refund;
                        network.send(call);
                    }, owner, Duration.ofMillis(100));
        }

        /** The crash points, aimed at this break's refund by what is committed (§9.4). */
        private void reached(CrashPoint point) {
            if (armed.filter(point::equals).isEmpty()) {
                return;
            }
            boolean strike = switch (point) {
                case CP1 -> count("kind = 'STATUS_QUERY_PAYMENT' AND status = 'DONE'") > 0 && count("kind = 'REFUND'") == 0;
                case CP2 -> count("kind = 'REFUND' AND status = 'PENDING'") == 1;
                case CP3 -> count("kind = 'REFUND' AND status = 'IN_FLIGHT'") == 1 && !refundSent;
                case CP4 -> refundJustSent;
                case CP5 -> recordingRefund;
                default -> false;
            };
            if (strike) {
                throw new Crash(point);
            }
        }

        private long count(String where) {
            return Containers.count(url, "SELECT count(*) FROM outbox WHERE break_id = '" + breakId + "' AND " + where);
        }

        /** Steps until {@code done} or the timeout; true if a crash point struck (the incarnation is then dead). */
        boolean run(BooleanSupplier done, Duration timeout) throws InterruptedException {
            Instant deadline = Instant.now().plus(timeout);
            while (!done.getAsBoolean() && Instant.now().isBefore(deadline)) {
                try {
                    resolver.stepOnce();
                    refundJustSent = false;
                    dispatcher.claimAndSend();
                    refundJustSent = false;
                    HttpNetwork.CallOutcome o = replies.poll(100, TimeUnit.MILLISECONDS);
                    while (o != null) {
                        recordingRefund = o.call().request() instanceof CallRequest.Refund;
                        dispatcher.record(o);
                        recordingRefund = false;
                        refundJustSent = false;
                        o = replies.poll();
                    }
                } catch (Crash crash) {
                    return true;
                }
            }
            return false;
        }
    }

    /** A crash point striking: the process dies there. */
    static final class Crash extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Crash(CrashPoint point) {
            super("crash at " + point, null, false, false);
        }
    }

    private static byte[] post(String uri) throws IOException, InterruptedException {
        HttpResponse<byte[]> r = HTTP.send(HttpRequest.newBuilder(URI.create(uri))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(r.statusCode()).as(new String(r.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        return r.body();
    }

    private static byte[] get(String uri) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create(uri)).GET().build(), HttpResponse.BodyHandlers.ofByteArray())
                .body();
    }
}
