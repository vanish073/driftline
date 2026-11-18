# The one Driftline image; DRIFTLINE_ROLE picks what it runs (DESIGN §3.3). Built from the Boot jar that Gradle made on
# the host (`./gradlew :app:bootJar`), so no build runs inside Docker. Runs as a non-root user and needs no writable
# path but /tmp, so the root filesystem can be read-only (D80).
#
# The OpenTelemetry Java agent (DESIGN §13, D78) is fetched from Maven Central and checked against its SHA-256 (whose
# SHA-1 matches the one Central publishes). It is loaded always and stays off unless OTEL_JAVAAGENT_ENABLED=true.
FROM eclipse-temurin:24.0.2_12-jre-noble@sha256:b416d02335e702b0403ff280de9475a3348e29382285969c9d4e17862ce632e7
# A fixed numeric user, so Kubernetes can check runAsNonRoot (D80).
RUN groupadd --system --gid 10001 driftline \
    && useradd --system --uid 10001 --gid driftline --no-create-home --shell /usr/sbin/nologin driftline \
    && mkdir -p /app/otel
WORKDIR /app
# The directory exists first, so --chmod applies to the jar alone (a directory ADD creates would get mode 444 too).
ADD --chmod=444 \
    --checksum=sha256:e43a270df53c6f423e0ab157be9c27ccc01d5dc3232e162fe54777573db2f149 \
    https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.33.1/opentelemetry-javaagent-2.33.1.jar \
    /app/otel/opentelemetry-javaagent.jar
COPY app/build/libs/app-0.1.0-SNAPSHOT.jar /app/driftline.jar
ENV OTEL_JAVAAGENT_ENABLED=false
USER 10001:10001
EXPOSE 8080 9091
ENTRYPOINT ["java", "-javaagent:/app/otel/opentelemetry-javaagent.jar", "-jar", "/app/driftline.jar"]
