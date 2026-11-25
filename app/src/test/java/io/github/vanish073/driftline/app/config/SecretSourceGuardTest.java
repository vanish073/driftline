package io.github.vanish073.driftline.app.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.ConfigTreePropertySource;
import org.springframework.core.env.MutablePropertySources;

/** D97: secrets come from files; a production start refuses them in the environment and never prints a value. */
class SecretSourceGuardTest {

    static final Map<String, String> SECRETS_IN_ENV = Map.of(
            "SPRING_DATASOURCE_PASSWORD", "test-only-db-password",
            "DRIFTLINE_SECRETS_KEYS_K1", "test-only-key",
            "driftline_pii_keys_p1", "test-only-key",
            "DRIFTLINE_PII_INDEX_KEY", "test-only-key",
            "DRIFTLINE_DISPATCHER_SERVICE_TOKEN", "test-only-token",
            "DRIFTLINE_ROLE", "ingest",
            "PATH", "/usr/bin");

    @Test
    void productionRefusesSecretsInTheEnvironmentNamingThemButNotTheirValues() {
        assertThatThrownBy(() -> SecretSourceGuard.check("production", SECRETS_IN_ENV))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SPRING_DATASOURCE_PASSWORD").hasMessageContaining("DRIFTLINE_SECRETS_KEYS_K1")
                .hasMessageContaining("driftline_pii_keys_p1").hasMessageContaining("DRIFTLINE_DISPATCHER_SERVICE_TOKEN")
                .hasMessageNotContaining("test-only").hasMessageNotContaining("DRIFTLINE_ROLE");
        assertThatThrownBy(() -> SecretSourceGuard.check("anything-else", Map.of("DRIFTLINE_PII_INDEX_KEY", "x")))
                .isInstanceOf(IllegalStateException.class);
        SecretSourceGuard.check("production", Map.of("DRIFTLINE_ROLE", "ingest", "DRIFTLINE_SECRETS_ACTIVE_KID", "k1",
                "DRIFTLINE_PII_ACTIVE_KID", "p1", "DRIFTLINE_SECRETS_DIR", "/run/secrets/driftline/"));
    }

    @Test
    void devAndTestStacksMayStillUseTheEnvironment() {
        SecretSourceGuard.check("dev", SECRETS_IN_ENV);
        SecretSourceGuard.check("test", SECRETS_IN_ENV);
    }

    @Test
    void aDirectoryOfFilesBindsToTheSameSettingsTheEnvironmentDid(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("driftline.secrets.keys.k1"), "dGVzdC1vbmx5LWtleS0xLW5vdC1hLXJlYWwta2V5LTAw\n");
        Files.writeString(dir.resolve("driftline.pii.keys.p1"), "dGVzdC1vbmx5LXBpaS1rZXktbm90LWEtcmVhbC1rZXk=");
        Files.writeString(dir.resolve("driftline.pii.index-key"), "test-only-index-key-not-a-real-key-000000\n");
        Files.writeString(dir.resolve("driftline.dispatcher.service-token"), "test-only-token\n");
        Files.writeString(dir.resolve("spring.datasource.password"), "test-only-db-password\n");
        MutablePropertySources sources = new MutablePropertySources();
        sources.addFirst(new ConfigTreePropertySource("secrets", dir,
                ConfigTreePropertySource.Option.AUTO_TRIM_TRAILING_NEW_LINE));
        sources.addLast(new org.springframework.core.env.MapPropertySource("config", Map.of(
                "driftline.secrets.active-kid", "k1", "driftline.pii.active-kid", "p1")));
        DriftlineProperties p = new Binder(ConfigurationPropertySources.from(sources)).bindOrCreate("driftline",
                DriftlineProperties.class);
        assertThat(p.secrets().keys()).containsOnlyKeys("k1")
                .containsEntry("k1", "dGVzdC1vbmx5LWtleS0xLW5vdC1hLXJlYWwta2V5LTAw");
        assertThat(p.pii().keys()).containsOnlyKeys("p1");
        assertThat(p.pii().indexKey()).isEqualTo("test-only-index-key-not-a-real-key-000000");
        assertThat(p.pii().configured()).isTrue();
        assertThat(p.dispatcher().serviceToken()).isEqualTo("test-only-token");
        assertThat(new Binder(ConfigurationPropertySources.from(sources)).bind("spring.datasource.password",
                String.class).get()).isEqualTo("test-only-db-password");
        assertThat(p.toString()).doesNotContain("dGVzdC1vbmx5").doesNotContain("test-only-index-key")
                .doesNotContain("test-only-token");
    }
}
