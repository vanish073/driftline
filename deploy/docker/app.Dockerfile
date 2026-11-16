# The one Driftline image; DRIFTLINE_ROLE picks what it runs (DESIGN §3.3). Built from the Boot jar that Gradle made on
# the host (`./gradlew :app:bootJar`), so no build runs inside Docker. Runs as a non-root user.
FROM eclipse-temurin:21.0.12.1_1-jre-noble@sha256:cd35bb33859641c5374415826a5951845104093eaa401cbe9d3d9023444f1c13
RUN groupadd --system driftline && useradd --system --gid driftline --no-create-home --shell /usr/sbin/nologin driftline
WORKDIR /app
COPY app/build/libs/app-0.1.0-SNAPSHOT.jar /app/driftline.jar
USER driftline
EXPOSE 8080 9091
ENTRYPOINT ["java", "-jar", "/app/driftline.jar"]
