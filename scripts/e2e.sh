#!/usr/bin/env bash
# End to end in Docker Compose (Milestones M3 and M4; DESIGN §15.1, §15.2, D51, D65).
#
#   scripts/e2e.sh [results-file]
#
# 1. Builds the Boot jar, the sandbox and the packaged e2e suite with Gradle, then stops Gradle to free memory.
# 2. Generates a fresh merchant pool (random webhook secrets and tokens, the dispatcher's service token) and random
#    database passwords and a secrets key under deploy/compose/.dev/ (git-ignored; nothing secret is ever committed).
# 3. Starts the Compose `core` profile and waits for the sandbox, which itself waits for a healthy Driftline.
# 4. Runs the e2e suite from the JUnit console launcher, with no Gradle running, and tees its output.
# 5. Always saves the Compose logs to deploy/compose/.dev/compose.log and tears the stack down, volumes included.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS=${1:-}
DEV=deploy/compose/.dev
COMPOSE=(docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core)
# shellcheck source=lib/compose-stack.sh
. scripts/lib/compose-stack.sh

stack_build :sandbox:e2eDist
stack_secrets 12
trap stack_teardown EXIT
stack_up

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
        stack_header scripts/e2e.sh "Compose profile core"
        run_suite
    } 2>&1 | tee "$RESULTS"
    exit "${PIPESTATUS[0]}"
else
    run_suite
fi
