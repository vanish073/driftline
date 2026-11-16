#!/usr/bin/env bash
# End to end in Docker Compose (Milestone M3; DESIGN §15.1, §15.2, D51).
#
#   scripts/e2e.sh [results-file]
#
# 1. Builds the Boot jar, the sandbox and the packaged e2e suite with Gradle, then stops Gradle to free memory.
# 2. Generates a fresh merchant pool (random webhook secrets and tokens) and random database passwords and a secrets key
#    under deploy/compose/.dev/ (git-ignored; nothing secret is ever committed).
# 3. Starts the Compose `core` profile and waits for the sandbox, which itself waits for a healthy Driftline.
# 4. Runs the e2e suite from the JUnit console launcher, with no Gradle running, and tees its output.
# 5. Always saves the Compose logs to deploy/compose/.dev/compose.log and tears the stack down, volumes included.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS=${1:-}
DEV=deploy/compose/.dev
COMPOSE=(docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core)

./gradlew :app:bootJar :sandbox:installDist :sandbox:e2eDist --quiet
./gradlew --stop --quiet || true

mkdir -p "$DEV"
sandbox/build/install/sandbox/bin/sandbox bootstrap --out "$DEV/bootstrap.json" --merchants 12 --partitions 12 \
    --days-back 3
umask 077
cat > "$DEV/env" <<ENV
POSTGRES_PASSWORD=$(openssl rand -hex 16)
DRIFTLINE_DB_PASSWORD=$(openssl rand -hex 16)
DRIFTLINE_SECRETS_KEY=$(openssl rand -base64 32)
ENV
umask 022

teardown() {
    local status=$?
    "${COMPOSE[@]}" logs --no-color --timestamps > "$DEV/compose.log" 2>&1 || true
    "${COMPOSE[@]}" down --volumes --remove-orphans > /dev/null 2>&1 || true
    exit "$status"
}
trap teardown EXIT

"${COMPOSE[@]}" up --detach --build
echo "waiting for the stack (sandbox up implies a healthy Driftline)..."
for _ in $(seq 1 150); do
    if curl -fsS http://127.0.0.1:8085/health > /dev/null 2>&1; then
        break
    fi
    if "${COMPOSE[@]}" ps --all --format '{{.Service}} {{.State}} {{.ExitCode}}' | grep -E '^(migrate|kafka-init) exited [1-9]'; then
        echo "a one-shot service failed" >&2
        exit 1
    fi
    sleep 2
done
curl -fsS http://127.0.0.1:8085/health > /dev/null
"${COMPOSE[@]}" ps

E2E=sandbox/build/e2e
CP="$E2E/classes:$(find "$E2E/lib" -name '*.jar' | sort | tr '\n' ':')"
run_suite() {
    BOOTSTRAP_FILE="$DEV/bootstrap.json" DRIFTLINE_URL=http://127.0.0.1:8080 SANDBOX_URL=http://127.0.0.1:8085 \
        java -Xmx192m -jar "$E2E"/launcher/junit-platform-console-standalone-*.jar execute \
        --class-path "$CP" --select-package io.github.vanish073.driftline.e2e --details=tree --disable-banner \
        --disable-ansi-colors
}
if [ -n "$RESULTS" ]; then
    mkdir -p "$(dirname "$RESULTS")"
    {
        echo "# scripts/e2e.sh, $(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD)$(git diff --quiet || echo '+uncommitted')"
        echo "# host: $(uname -sr), $(nproc) cores, $(free -g | awk '/Mem:/ {print $2}') GB RAM; Compose profile core"
        run_suite
    } 2>&1 | tee "$RESULTS"
    exit "${PIPESTATUS[0]}"
else
    run_suite
fi
