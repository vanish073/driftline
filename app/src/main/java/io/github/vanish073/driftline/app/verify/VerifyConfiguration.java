package io.github.vanish073.driftline.app.verify;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.config.DriftlineProperties;
import io.github.vanish073.driftline.core.audit.AuditChain;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * The one-shot check roles: {@code verify} runs {@link Verify} once, {@code verify-audit} walks the audit chain
 * ({@link AuditChain}, §7.6); each fails the process if its check does not pass.
 */
@Configuration
@Profile(Roles.VERIFY + " | " + Roles.VERIFY_AUDIT)
public class VerifyConfiguration {

    @Bean
    @Profile(Roles.VERIFY_AUDIT)
    ApplicationRunner verifyAudit(DataSource dataSource) {
        return args -> {
            AuditChain.Verdict v = Verify.auditChain(dataSource);
            System.out.println("verify-audit: " + v.rows() + " rows; " + v.reason());
            if (!v.intact()) {
                throw new IllegalStateException("driftline verify-audit: the audit chain is broken at row "
                        + v.firstBadSeq().orElseThrow());
            }
        };
    }

    @Bean
    @Profile(Roles.VERIFY)
    ApplicationRunner verify(DataSource dataSource, DriftlineProperties properties) {
        return args -> {
            DriftlineProperties.Verify v = properties.verify();
            if (!new Verify(dataSource, Optional.ofNullable(v.sandboxUrl())).run(v.waitFor(), v.every(), System.out)) {
                throw new IllegalStateException("driftline verify found violations");
            }
        };
    }
}
