package io.github.vanish073.driftline.app.config;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.merchant.DevBootstrap;
import io.github.vanish073.driftline.app.merchant.MerchantRegistry;
import io.github.vanish073.driftline.app.merchant.SecretCipher;
import io.github.vanish073.driftline.app.merchant.SecretRefresher;
import io.github.vanish073.driftline.app.pii.CustomerContacts;
import io.github.vanish073.driftline.app.pii.PiiCipher;
import io.github.vanish073.driftline.app.port.SystemClock;
import io.github.vanish073.driftline.core.hooks.Canaries;
import io.github.vanish073.driftline.core.hooks.CrashPoints;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.CustomerVault;
import io.github.vanish073.driftline.store.postgres.PostgresDirectory;
import io.github.vanish073.driftline.store.postgres.PostgresStore;
import io.github.vanish073.driftline.store.postgres.SettlementInbox;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * The composition root of production (D17, DESIGN §3.2): the real Clock, Network and Store, built here and handed to
 * the core by constructor. The core has no Spring annotations and is never scanned; this file and
 * {@link RoleConfiguration} are the whole wiring, the production counterpart of the simulator's {@code Simulation}.
 */
@Configuration
@Profile(Roles.SERVICE)
public class CoreConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(CoreConfiguration.class);

    @Bean
    Clock clock() {
        return new SystemClock();
    }

    /** Production never runs a canary (§11.10). {@link #canaryGuard} refuses to start if any other value is present. */
    @Bean
    Canaries canaries() {
        return Canaries.NONE;
    }

    @Bean
    static SmartInitializingSingleton canaryGuard(ConfigurableListableBeanFactory beans) {
        return () -> beans.getBeansOfType(Canaries.class).forEach((name, canaries) -> {
            if (!canaries.isNone()) {
                throw new IllegalStateException("canaries " + canaries.enabled() + " are switched on in bean " + name
                        + "; production must run with Canaries.NONE (DESIGN §11.10)");
            }
        });
    }

    @Bean
    CrashPoints crashPoints() {
        return CrashPoints.NONE;
    }

    /** The one Store (§3.2); the core and the drivers see it only through the {@code Store} port. */
    @Bean(destroyMethod = "")
    PostgresStore store(DataSource dataSource, DriftlineProperties properties) {
        return new PostgresStore(dataSource, properties.store().balanceStrategy());
    }

    @Bean
    PostgresDirectory postgresDirectory(DataSource dataSource) {
        return new PostgresDirectory(dataSource);
    }

    @Bean
    SettlementInbox settlementInbox(DataSource dataSource) {
        return new SettlementInbox(dataSource);
    }

    @Bean
    SecretCipher secretCipher(DriftlineProperties properties) {
        return new SecretCipher(properties.secrets().activeKid(), properties.secrets().keys());
    }

    @Bean
    CustomerVault customerVault(DataSource dataSource) {
        return new CustomerVault(dataSource);
    }

    /** Customers' contact details (D77): stored encrypted when the personal-data keys are configured, else discarded. */
    @Bean
    CustomerContacts customerContacts(CustomerVault vault, DriftlineProperties properties) {
        DriftlineProperties.Pii pii = properties.pii();
        if (!pii.configured()) {
            LOG.warn("personal data storage is off (no driftline.pii keys): customer contacts on webhooks are "
                    + "discarded");
            return new CustomerContacts(vault, Optional.empty());
        }
        return new CustomerContacts(vault, Optional.of(new PiiCipher(pii.activeKid(), pii.keys(), pii.indexKey())));
    }

    /** Reloads the active webhook secrets while running (D76). */
    @Bean
    SecretRefresher secretRefresher(MerchantRegistry registry, PostgresDirectory directory, SecretCipher cipher,
                                    Clock clock, DriftlineProperties properties) {
        return new SecretRefresher(registry, directory, cipher, clock, properties.secrets().refreshInterval());
    }

    /** Applies the dev bootstrap file first, if one is configured (D49), then loads every merchant. */
    @Bean
    MerchantRegistry merchantRegistry(PostgresDirectory directory, SecretCipher cipher, Clock clock,
                                      DriftlineProperties properties) {
        properties.bootstrap().ifPresent(file -> DevBootstrap.apply(file, directory, cipher, clock.now()));
        MerchantRegistry registry = MerchantRegistry.load(directory, cipher, clock.now());
        LOG.info("serving {} merchants", registry.merchants().size());
        return registry;
    }
}
