#!/usr/bin/env bash
# claims-lint (DESIGN §16, §2.2): the claim-bearing files must never claim what Driftline does not guarantee.
# Scans README.md, docs/results/** and the deployment manifests (deploy/**). Fails on "exactly-once", "exactly once",
# "zero data loss" or "guaranteed delivery", in any case, outside a block opened by <!-- claims:allow --> and closed
# by <!-- claims:end -->. The only such block is the README section that explains why Driftline does not claim it.
# Design and spec documents are not scanned: they must discuss the terms in order to disclaim them.
set -euo pipefail
cd "$(dirname "$0")/.."

PATTERN='exactly[- ]once|zero data loss|guaranteed delivery'
files=()
[ -f README.md ] && files+=(README.md)
while IFS= read -r -d '' f; do files+=("$f"); done < <(find docs/results deploy -type f -print0 2>/dev/null)

status=0
for f in "${files[@]}"; do
    hits=$(awk -v pat="$PATTERN" '
        /<!-- claims:allow -->/ { allowed = 1; next }
        /<!-- claims:end -->/   { allowed = 0; next }
        !allowed && tolower($0) ~ pat { printf "%s:%d: %s\n", FILENAME, FNR, $0 }
    ' "$f")
    if [ -n "$hits" ]; then
        echo "$hits"
        status=1
    fi
done
if [ "$status" -ne 0 ]; then
    echo "claims-lint: write \"effectively-once effects\" and link DESIGN §2 instead (CLAUDE.md, DESIGN §16)." >&2
else
    echo "claims-lint: ${#files[@]} claim-bearing files clean"
fi
exit "$status"
