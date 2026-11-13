package io.github.vanish073.driftline.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The Spring Boot entry point (D17). Component scanning covers {@code io.github.vanish073.driftline.app} only; the
 * core is never scanned. From Shift 6, plain {@code @Configuration} classes here wire the real Clock, Network and
 * Store into the core explicitly.
 */
@SpringBootApplication
public class DriftlineApplication {

    public static void main(String[] args) {
        SpringApplication.run(DriftlineApplication.class, args);
    }
}
