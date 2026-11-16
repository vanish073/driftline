# The sandbox (DESIGN §15.2, D20, D51), from `./gradlew :sandbox:installDist`. Runs as a non-root user.
FROM eclipse-temurin:21.0.12.1_1-jre-noble@sha256:cd35bb33859641c5374415826a5951845104093eaa401cbe9d3d9023444f1c13
RUN groupadd --system sandbox && useradd --system --gid sandbox --no-create-home --shell /usr/sbin/nologin sandbox
WORKDIR /app
COPY sandbox/build/install/sandbox /app
USER sandbox
EXPOSE 8085
ENTRYPOINT ["/app/bin/sandbox"]
