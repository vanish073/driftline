#!/usr/bin/env bash
# The cumulative simulator seed count (DESIGN §17, D85): every campaign shard's summary, from GitHub Actions artifacts
# and from the committed local campaigns, added up by scripts/seed-count.py.
#
#   scripts/seed-count.sh [out-dir]       default docs/results; writes seed-count.json and seed-count.md
#
# Needs `gh` logged in. Lists the runs of ci.yml and nightly.yml, downloads each run's `sim-summary-*` artifacts
# (runs before Shift 10 have none, and artifacts expire after 90 days: the committed seed-count.json keeps the record),
# and fails if two summaries cover the same seeds.
set -euo pipefail
cd "$(dirname "$0")/.."
OUTDIR=${1:-docs/results}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/artifacts" "$OUTDIR/sim-summaries"
for wf in ci.yml nightly.yml; do
    gh run list --workflow "$wf" --limit 1000 \
        --json databaseId,workflowName,createdAt,headSha,headBranch,event,conclusion > "$work/runs-$wf.json"
done
python3 -I -c '
import json, sys
print(json.dumps(json.load(open(sys.argv[1])) + json.load(open(sys.argv[2]))))
' "$work/runs-ci.yml.json" "$work/runs-nightly.yml.json" > "$work/runs.json"
for id in $(python3 -I -c 'import json, sys; [print(r["databaseId"]) for r in json.load(open(sys.argv[1]))]' \
        "$work/runs.json"); do
    gh run download "$id" --pattern 'sim-summary-*' --dir "$work/artifacts/$id" > /dev/null 2>&1 || true
done
python3 scripts/seed-count.py "$work/runs.json" "$work/artifacts" "$OUTDIR/sim-summaries" \
    "$OUTDIR/seed-count.json" "$OUTDIR/seed-count.md"
