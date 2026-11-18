#!/usr/bin/env bash
# The Gradle lockfiles (D79) match what the build files declare.
#
#   scripts/check-lockfiles.sh           fails if re-resolving would change any committed lockfile (CI)
#   scripts/check-lockfiles.sh --write   rewrites them, after a version change
#
# The check exists because a lockfile pins a version a BOM manages without complaint: bumping Spring Boot without new
# locks would keep building with the old versions.
set -euo pipefail
cd "$(dirname "$0")/.."

mapfile -t projects < <(./gradlew -q projects | grep -o "Project ':[a-z-]*'" | sed "s/Project '\(.*\)'/\1/")
if [ "${#projects[@]}" -eq 0 ]; then
    echo "check-lockfiles: found no Gradle projects" >&2
    exit 1
fi
tasks=(dependencies)
for p in "${projects[@]}"; do
    tasks+=("$p:dependencies")
done
./gradlew -q "${tasks[@]}" --write-locks > /dev/null
if [ "${1:-}" = "--write" ]; then
    echo "check-lockfiles: rewrote the lockfiles of ${#projects[@]} projects and the settings"
    exit 0
fi
if ! git diff --exit-code --stat -- '*.lockfile' || [ -n "$(git ls-files --others --exclude-standard -- '*.lockfile')" ]; then
    echo "check-lockfiles: the lockfiles above are out of date; run scripts/check-lockfiles.sh --write and commit" >&2
    exit 1
fi
echo "check-lockfiles: ${#projects[@]} module lockfiles and the settings lockfile match the build files"
