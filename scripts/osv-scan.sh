#!/usr/bin/env bash
# OSV-Scanner over the committed Gradle lockfiles (DESIGN §14, D79), run by CI's `security` job; needs Docker.
# Fails on any advisory whose highest CVSS score is 7.0 or more (high or critical), and on any advisory without a score,
# since it cannot be shown to be lower. Every finding is printed, whatever its score.
set -euo pipefail
cd "$(dirname "$0")/.."

OSV=ghcr.io/google/osv-scanner:v2.7.0@sha256:4f3b7e2c64b9d38a5febc5a7e4a3dfef1d1362f726dea7a2ef4762b46469fcd0
mkdir -p build
lockfiles=()
for f in */gradle.lockfile; do
    lockfiles+=(-L "/src/$f")
done
status=0
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/src" "$OSV" scan source --format json \
    --output /src/build/osv.json "${lockfiles[@]}" > build/osv.log 2>&1 || status=$?
if [ "$status" -gt 1 ]; then   # 1 means advisories were found; anything higher is a scanner error
    cat build/osv.log >&2
    exit "$status"
fi
python3 -I - build/osv.json "$((${#lockfiles[@]} / 2))" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    report = json.load(f)
blocking = 0
seen = set()
for result in report.get("results", []):
    path = result["source"]["path"].replace("/src/", "")
    for pkg in result.get("packages", []):
        name, version = pkg["package"]["name"], pkg["package"]["version"]
        for group in pkg.get("groups", []):
            key = (name, version, tuple(group["ids"]))
            if key in seen:
                continue
            seen.add(key)
            score = group.get("max_severity") or ""
            block = score == "" or float(score) >= 7.0
            blocking += block
            print(f"osv-scan: {'BLOCKING' if block else 'note    '} {name}:{version} {','.join(group['ids'])}"
                  f" max CVSS {score or 'none'} ({path})")
if blocking:
    print(f"osv-scan: FAIL: {blocking} advisories are high, critical or unscored", file=sys.stderr)
    sys.exit(1)
print(f"osv-scan: no high, critical or unscored advisory in {sys.argv[2]} lockfiles")
PY
