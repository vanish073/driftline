package io.github.vanish073.driftline.app.config;

import io.github.vanish073.driftline.app.Roles;
import io.github.vanish073.driftline.app.observability.BookMetrics;
import io.github.vanish073.driftline.app.observability.DispatcherObserver;
import io.github.vanish073.driftline.app.observability.IngestMetrics;
import io.github.vanish073.driftline.app.observability.MatcherObserver;
import io.github.vanish073.driftline.app.observability.ResolverObserver;
import io.github.vanish073.driftline.app.observability.Tracing;
import io.github.vanish073.driftline.core.port.Clock;
import io.github.vanish073.driftline.store.postgres.BookStats;
import io.github.vanish073.driftline.store.postgres.TraceContexts;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Metrics and traces (DESIGN §13, D78), wired explicitly like every other port. The OpenTelemetry implementation is the
 * Java agent's when it runs (it installs the global instance), an {@link OpenTelemetry} bean if a test registers one,
 * and otherwise the no-op API, with tracing off.
 */
@Configuration
@Profile(Roles.SERVICE)
public class ObservabilityConfiguration {

    @Bean
    Tracing tracing(ObjectProvider<OpenTelemetry> openTelemetry) {
        return new Tracing(openTelemetry.getIfAvailable(GlobalOpenTelemetry::get));
    }

    @Bean
    TraceContexts traceContexts(DataSource dataSource) {
        return new TraceContexts(dataSource);
    }

    @Bean
    @Profile(Roles.INGEST)
    IngestMetrics ingestMetrics(MeterRegistry registry) {
        return new IngestMetrics(registry);
    }

    /** Ahead of Spring Security, so refused requests are counted too. */
    @Bean
    @Profile(Roles.INGEST)
    FilterRegistrationBean<OncePerRequestFilter> ingestMetricsFilter(IngestMetrics metrics) {
        FilterRegistrationBean<OncePerRequestFilter> registration = new FilterRegistrationBean<>(metrics.filter());
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    @Profile(Roles.MATCHER)
    MatcherObserver matcherObserver(MeterRegistry registry, Tracing tracing, TraceContexts traces) {
        return new MatcherObserver(registry, tracing, traces);
    }

    @Bean
    @Profile(Roles.RESOLVER)
    ResolverObserver resolverObserver(Tracing tracing, TraceContexts traces) {
        return new ResolverObserver(tracing, traces);
    }

    @Bean
    @Profile(Roles.DISPATCHER)
    DispatcherObserver dispatcherObserver(MeterRegistry registry, Tracing tracing, TraceContexts traces) {
        return new DispatcherObserver(registry, tracing, traces);
    }

    /** The books' metrics are read by the resolver role, so each deployment reports them from one place (D78). */
    @Bean
    @Profile(Roles.RESOLVER)
    BookMetrics bookMetrics(MeterRegistry registry, DataSource dataSource, Clock clock,
                            DriftlineProperties properties) {
        return new BookMetrics(registry, new BookStats(dataSource), clock, properties.resolver().metricsInterval());
    }
}
