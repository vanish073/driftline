#!/usr/bin/env bash
# Resolves the build's dependencies and compiles the given tasks, retrying on failure (D85): when dozens of CI jobs
# start together, Maven Central may answer a download nobody has cached yet with 429 Too Many Requests. Only this
# cheap step is retried, never a campaign; a real compile error fails all five tries and the job.
#
#   scripts/gradle-warm.sh --configure-on-demand :sim:classes
set -uo pipefail
cd "$(dirname "$0")/.."

for try in 1 2 3 4 5; do
    if ./gradlew -q "$@"; then
        exit 0
    fi
    if [ "$try" = 5 ]; then
        echo "gradle-warm: $* failed 5 times" >&2
        exit 1
    fi
    wait_s=$(( 30 * try + RANDOM % 30 ))
    echo "gradle-warm: $* failed (try $try of 5); trying again in $wait_s s" >&2
    sleep "$wait_s"
done
