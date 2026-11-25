#!/usr/bin/env bash
# Shift 9's observability evidence (DESIGN §13, D78; SHIFTS Shift 9 "Prove"): the Compose `core` and `obs` profiles
# with tracing on, under a few minutes of sandbox traffic, then:
#   - a screenshot of the Grafana board showing that traffic;
#   - one trace that runs parent to child from the webhook to the dispatcher's call: its JSON, and a screenshot of it
#     in Jaeger;
#   - the Prometheus numbers behind the board, and scripts/obs-check.sh.
#
#   scripts/obs-evidence.sh [out-dir]     default docs/results; EVIDENCE_ROUNDS (default 3), EVIDENCE_SECONDS (300)
#
# Writes obs-evidence-shift9.txt, grafana-shift9.png, jaeger-trace-shift9.png and jaeger-trace-shift9.json. Needs
# about 2.8 GB: close VS Code first, as for scripts/e2e.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

OUTDIR=${1:-docs/results}
ROUNDS=${EVIDENCE_ROUNDS:-3}
DURATION=${EVIDENCE_SECONDS:-300}
DEV=deploy/compose/.dev
COMPOSE=(docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core --profile obs)
TYPES=(DEBITED_NO_ORDER LATE_SUCCESS DUPLICATE_CAPTURE SHORT_SETTLEMENT MISSING_SETTLEMENT ORPHAN_SETTLEMENT NORMAL)
# Playwright's image (its browsers match playwright 1.64.1, which scripts/lib/screenshot-requirements.txt pins)
BROWSER=mcr.microsoft.com/playwright/python:v1.64.1-noble@sha256:330d401ae23c905896a0c26c66c36fc971b621519e7b11b435a8328d7bf6e031
# shellcheck source=lib/compose-stack.sh
. scripts/lib/compose-stack.sh

mkdir -p "$OUTDIR"
exec > >(tee "$OUTDIR/obs-evidence-shift9.txt") 2>&1
stack_header scripts/obs-evidence.sh "Compose profiles core and obs, tracing on; EVIDENCE_ROUNDS=$ROUNDS EVIDENCE_SECONDS=$DURATION"

stack_build
stack_secrets $((ROUNDS * ${#TYPES[@]} + 2)) "DRIFTLINE_TRACING=true"
trap stack_teardown EXIT
stack_up

echo "evidence: $ROUNDS rounds of ${#TYPES[@]} injections (count 5) over $DURATION s"
traffic_from=$(date +%s)
gap=$((DURATION / (ROUNDS * ${#TYPES[@]})))
for round in $(seq 1 "$ROUNDS"); do
    for type in "${TYPES[@]}"; do
        curl -fsS --cacert "$DEV/secrets/ca.crt" -X POST "https://127.0.0.1:8085/v1/inject?type=$type&count=5" > /dev/null
        echo "evidence: injected $type (round $round)"
        sleep "$gap"
    done
done
echo "evidence: traffic done; letting the system settle and Prometheus scrape for 90 s"
sleep 90

scripts/obs-check.sh | tee "$DEV/obs-check.out"
trace=$(sed -n 's/^obs-check: Jaeger trace \([0-9a-f]\{32\}\):.*/\1/p' "$DEV/obs-check.out")
if [ -z "$trace" ]; then
    echo "evidence: FAIL: no full-chain trace to show" >&2
    exit 1
fi

echo "evidence: the numbers behind the board (Prometheus, at the end of the run)"
prometheus_auth="prometheus:$(cat "$DEV/secrets/grafana/prometheus-password")"
grafana_auth="admin:$(cat "$DEV/secrets/grafana/admin-password")"
query() {
    curl -fsS -u "$prometheus_auth" --get --data-urlencode "query=$1" http://127.0.0.1:9090/api/v1/query | python3 -I -c '
import json, sys
label = sys.argv[1]
for r in json.load(sys.stdin)["data"]["result"]:
    tags = ",".join(f"{k}={v}" for k, v in sorted(r["metric"].items()) if k not in ("__name__", "instance", "job"))
    value = r["value"][1]
    print(f"  {label}{{{tags}}} = {value}")' "$2"
}
query 'sum by (source) (driftline_events_consumed_total)' 'events consumed'
query 'sum by (route, status) (driftline_ingest_requests_total)' 'ingest requests'
query 'histogram_quantile(0.5, sum by (le) (rate(driftline_event_to_match_seconds_bucket[30m])))' 'event-to-match p50 (s)'
query 'histogram_quantile(0.99, sum by (le) (rate(driftline_event_to_match_seconds_bucket[30m])))' 'event-to-match p99 (s)'
query 'max by (type) (driftline_breaks_detected_total)' 'breaks detected'
query 'max by (type, how) (driftline_breaks_closed_total)' 'breaks closed'
query 'max by (kind, result) (driftline_outbox_attempts_total)' 'outbox attempts'
query 'max(driftline_compensation_exposure_rupees)' 'compensation exposure (INR)'
echo "evidence: these times are for injected traffic whose events lie in the past (D51); they are not latency results"

# Jaeger is not published (D98): its API through Grafana's datasource proxy
curl -fsS -u "$grafana_auth" "http://127.0.0.1:3000/api/datasources/proxy/uid/jaeger/api/v3/traces/$trace" \
    | python3 -I -m json.tool > "$OUTDIR/jaeger-trace-shift9.json"
echo "evidence: saved trace $trace to $OUTDIR/jaeger-trace-shift9.json"

# Screenshots from a headless browser on the Compose network (scripts/lib/screenshot.py, Playwright installed by hash),
# each waiting until the page shows what it must, and each stopped after 6 minutes at most.
network=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$("${COMPOSE[@]}" ps -q grafana)")
shot() {
    local out=$1 url=$2 size=$3 selector=$4 minimum=$5 texts=$6 name status=0
    name="driftline-shot-$$-${out%.png}"
    timeout 360 docker run --rm --name "$name" --ipc=host --network "$network" -v "$PWD/scripts/lib:/work:ro" \
        -v "$PWD/$OUTDIR:/out" --user "$(id -u):$(id -g)" -e HOME=/tmp -e SHOT_AUTH="${SHOT_AUTH:-}" \
        --entrypoint sh "$BROWSER" -c \
        'pip install --quiet --no-cache-dir --user --require-hashes --only-binary=:all: \
             -r /work/screenshot-requirements.txt 2> /dev/null \
         && python3 /work/screenshot.py "$0" "$1" "$2" "$3" "$4" "$5" "$6"' \
        "$url" "/out/$out" "${size%,*}" "${size#*,}" "$selector" "$minimum" "$texts" || status=$?
    docker rm -f "$name" > /dev/null 2>&1 || true
    if [ "$status" -ne 0 ] || [ ! -s "$OUTDIR/$out" ]; then
        echo "evidence: FAIL: no screenshot $out (status $status)" >&2
        return 1
    fi
    echo "evidence: saved $OUTDIR/$out ($(wc -c < "$OUTDIR/$out") bytes)"
}
SHOT_AUTH="$grafana_auth" shot grafana-shift9.png \
    "http://grafana:3000/d/driftline/driftline?orgId=1&kiosk&from=$((traffic_from - 60))000&to=$(date +%s)000&refresh=" \
    1600,2600 canvas 15 "Events consumed per second"
shot jaeger-trace-shift9.png "http://jaeger:16686/trace/$trace" 1600,1100 body 1 \
    "ingest.webhook,matcher.detect,resolver.decide,dispatcher.call"
echo "evidence: done"
