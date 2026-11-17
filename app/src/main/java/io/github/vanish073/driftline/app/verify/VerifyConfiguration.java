package io.github.vanish073.driftline.app.verify;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.config.DriftlineProperties;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** The {@code verify} role: runs {@link Verify} once and fails the process if a check does not pass. */
@Configuration
@Profile(Roles.VERIFY)
public class VerifyConfiguration {

    @Bean
    ApplicationRunner verify(DataSource dataSource, DriftlineProperties properties) {
        return args -> {
            DriftlineProperties.Verify v = properties.verify();
            if (!new Verify(dataSource, Optional.ofNullable(v.sandboxUrl())).run(v.waitFor(), v.every(), System.out)) {
                throw new IllegalStateException("driftline verify found violations");
            }
        };
    }
}
