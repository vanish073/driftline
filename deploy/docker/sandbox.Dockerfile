# The sandbox (DESIGN §15.2, D20, D51), from `./gradlew :sandbox:installDist`. Runs as a non-root user.
FROM eclipse-temurin:24.0.2_12-jre-noble@sha256:b416d02335e702b0403ff280de9475a3348e29382285969c9d4e17862ce632e7
RUN groupadd --system --gid 10001 sandbox \
    && useradd --system --uid 10001 --gid sandbox --no-create-home --shell /usr/sbin/nologin sandbox
WORKDIR /app
COPY sandbox/build/install/sandbox /app
USER 10001:10001
EXPOSE 8085
ENTRYPOINT ["/app/bin/sandbox"]
