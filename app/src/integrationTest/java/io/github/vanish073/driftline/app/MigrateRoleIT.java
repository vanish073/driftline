package io.github.vanish073.driftline.app;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

/** DRIFTLINE_ROLE=migrate: the one-shot service of the Compose stack runs every migration and has no web server. */
class MigrateRoleIT {

    @Test
    void theMigrateRoleMigratesAnEmptyDatabaseAndStartsNothingElse() {
        String url = Containers.emptyDatabase("migrate_role_it");
        try (ConfigurableApplicationContext ctx = DriftlineApplication.application("migrate").run(
                "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + Containers.POSTGRES.getUsername(),
                "--spring.datasource.password=" + Containers.POSTGRES.getPassword())) {
            assertThat(ctx.getEnvironment().getProperty("local.server.port")).as("no web server").isNull();
            assertThat(ctx.getBeanNamesForType(io.github.vanish073.driftline.core.port.Store.class)).isEmpty();
        }
        assertThat(Containers.count(url, "SELECT count(*) FROM flyway_schema_history WHERE success"))
                .isEqualTo(5);
        assertThat(Containers.count(url, "SELECT count(*) FROM information_schema.tables WHERE table_name IN "
                + "('merchants', 'api_tokens', 'merchant_webhook_secrets', 'settlement_uploads')")).isEqualTo(4);
    }
}
