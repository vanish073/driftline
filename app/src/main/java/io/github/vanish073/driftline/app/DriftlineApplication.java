package io.github.vanish073.driftline.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * The Spring Boot entry point (D17). Component scanning covers {@code io.github.vanish073.driftline.app} only; the
 * core is never scanned. Plain {@code @Configuration} classes in {@code app.config} build the real Clock, Network and
 * Store and hand them to the core explicitly. {@code DRIFTLINE_ROLE} picks the Spring profiles ({@link Roles}).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DriftlineApplication {

    public static void main(String[] args) {
        String role = System.getenv().getOrDefault("DRIFTLINE_ROLE", "all");
        application(role).run(args);
    }

    /** The application for {@code role}, with its profiles; tests start it the same way. */
    public static SpringApplication application(String role) {
        SpringApplication app = new SpringApplication(DriftlineApplication.class);
        app.setAdditionalProfiles(Roles.profiles(role).toArray(String[]::new));
        return app;
    }
}
