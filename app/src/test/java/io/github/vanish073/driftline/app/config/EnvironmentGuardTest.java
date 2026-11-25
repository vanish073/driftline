package io.github.vanish073.driftline.app.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/** D97: the dev bootstrap file, which creates merchants and tokens from plain text, is refused outside dev and test. */
class EnvironmentGuardTest {

    static DriftlineProperties bind(Map<String, String> settings) {
        return new Binder(new MapConfigurationPropertySource(settings)).bindOrCreate("driftline",
                DriftlineProperties.class);
    }

    @Test
    void productionIsTheDefaultAndRefusesABootstrapFileBeforeTouchingTheDatabase() {
        PostgresDirectory directory = mock(PostgresDirectory.class);
        Clock clock = mock(Clock.class);
        when(clock.now()).thenReturn(Instant.parse("2026-11-25T10:00:00Z"));
        DriftlineProperties unset = bind(Map.of("driftline.bootstrap-file", "/bootstrap/bootstrap.json"));
        assertThat(unset.environment()).isEqualTo("production");
        assertThat(unset.devOrTest()).isFalse();
        for (DriftlineProperties p : List.of(unset, bind(Map.of("driftline.bootstrap-file", "/b.json",
                "driftline.environment", "production")))) {
            assertThatThrownBy(() -> new CoreConfiguration().merchantRegistry(directory, mock(SecretCipher.class),
                    clock, p)).isInstanceOf(IllegalStateException.class).hasMessageContaining("dev and test stacks only");
        }
        assertThatThrownBy(() -> new CoreConfiguration().merchantRegistry(directory, mock(SecretCipher.class), clock,
                bind(Map.of("driftline.environment", "staging")))).hasMessageContaining("must be one of");
        verifyNoInteractions(directory);
    }

    @Test
    void devAndTestMayUseIt() {
        assertThat(bind(Map.of("driftline.environment", "dev")).devOrTest()).isTrue();
        assertThat(bind(Map.of("driftline.environment", "test")).devOrTest()).isTrue();
        assertThat(bind(Map.of("driftline.environment", "Dev")).devOrTest()).as("exact names only").isFalse();
    }
}
