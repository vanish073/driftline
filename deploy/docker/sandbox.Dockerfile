# The sandbox (DESIGN §15.2, D20, D51), from `./gradlew :sandbox:installDist`. Runs as a non-root user.
FROM eclipse-temurin:21.0.13_9-jre-noble@sha256:0d57a8e90c2bb844c5a8e1b2b71e11eff06dad7edcfd803cd21ed4ae203b2515
RUN groupadd --system sandbox && useradd --system --gid sandbox --no-create-home --shell /usr/sbin/nologin sandbox
WORKDIR /app
COPY sandbox/build/install/sandbox /app
USER sandbox
EXPOSE 8085
ENTRYPOINT ["/app/bin/sandbox"]
