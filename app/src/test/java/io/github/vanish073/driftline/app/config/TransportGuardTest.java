package io.github.vanish073.driftline.app.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

/** D100: a production start refuses any of Driftline's links without TLS, naming each one. */
class TransportGuardTest {

    static MockEnvironment secured() {
        return new MockEnvironment()
                .withProperty("spring.datasource.url",
                        "jdbc:postgresql://postgres:5432/driftline?sslmode=verify-full&sslrootcert=/run/tls/ca.crt")
                .withProperty("driftline.kafka.security-protocol", "SASL_SSL")
                .withProperty("server.ssl.bundle", "server")
                .withProperty("management.server.ssl.bundle", "server")
                .withProperty("driftline.dispatcher.gateway-url", "https://gateway.example/v1")
                .withProperty("driftline.dispatcher.observations-url", "https://ingest:8080");
    }

    @Test
    void aFullySecuredServicePassesAndEachPlaintextLinkIsNamed() {
        TransportGuard.check("ingest", secured(), Map.of());
        MockEnvironment plain = new MockEnvironment()
                .withProperty("spring.datasource.url", "jdbc:postgresql://postgres:5432/driftline")
                .withProperty("driftline.dispatcher.gateway-url", "http://gateway.example/v1");
        assertThat(TransportGuard.problems("all", plain, Map.of("OTEL_JAVAAGENT_ENABLED", "true",
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")))
                .containsExactly("spring.datasource.url must ask for sslmode=verify-full",
                        "driftline.kafka.security-protocol must be SASL_SSL",
                        "server.ssl.bundle must name an SSL bundle (the API serves HTTPS)",
                        "management.server.ssl.bundle must name an SSL bundle (health and metrics serve HTTPS)",
                        "driftline.dispatcher.gateway-url must be https",
                        "OTEL_EXPORTER_OTLP_ENDPOINT must be https while tracing is on");
        assertThatThrownBy(() -> TransportGuard.check("all", plain, Map.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("TLS on every link");
    }

    @Test
    void weakerSslModesAreNotEnoughAndOneTimeRolesCheckOnlyTheDatabase() {
        for (String mode : java.util.List.of("disable", "allow", "prefer", "require", "verify-ca")) {
            MockEnvironment e = secured().withProperty("spring.datasource.url",
                    "jdbc:postgresql://postgres:5432/driftline?sslmode=" + mode);
            assertThat(TransportGuard.problems("ingest", e, Map.of())).as(mode).hasSize(1);
        }
        MockEnvironment migrate = new MockEnvironment().withProperty("spring.datasource.url",
                "jdbc:postgresql://postgres:5432/driftline?sslmode=verify-full&sslrootcert=/run/tls/ca.crt");
        TransportGuard.check("migrate", migrate, Map.of());
        TransportGuard.check("verify", migrate, Map.of());
    }

    @Test
    void devAndTestStacksMayRunPlaintext() {
        TransportGuard.check("all", new MockEnvironment().withProperty("driftline.environment", "test"), Map.of());
        TransportGuard.check("all", new MockEnvironment().withProperty("driftline.environment", "dev"), Map.of());
    }
}
