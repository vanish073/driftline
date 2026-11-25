#!/usr/bin/env bash
# The load test (DESIGN §17; REQUIREMENTS "events per second sustained on your laptop, and p99 time from event to
# match"): the open-model generator (bench LoadGenerator) against the Compose `core` profile, plus Prometheus alone.
#
#   scripts/load-test.sh [out-dir]       default docs/results
#     LOAD_RATES         offered rates, events/s, tried in order until one is not sustained (default "25 50 100 200 400")
#     LOAD_STEP_SECONDS  length of each step (default 600: 10 minutes, judged on its last 5)
#
# 1. Builds the jar, the sandbox and the generator, then stops Gradle; fresh secrets and 12 merchants whose first
#    settlement date is two days ahead, so no settlement alert fires during the run.
# 2. Starts the stack and Prometheus (only it, from the `obs` profile), and records vmstat with an epoch per line and
#    each container's CPU and memory (`docker stats`, a sample every few seconds), so a busy machine can be attributed.
# 3. Steps up the offered rate. A step is sustained when, over its last 5 minutes, the consumer lag (the backlog
#    probe's, D83) did not grow and the generator saw no rejection, error, skip or unanswered request
#    (scripts/load-evaluate.py). It stops at the first step that is not, or that is INVALID because the machine
#    paused or swapped (at least 256 KB/s for 5 s in a row): then the limit was not found, and the summary says so.
# 4. Runs two more steps at 50% and 80% of the highest sustained rate, for the p99 event-to-match at each.
# 5. Writes load-step-<rate>.json per step, load-p99-50pct.json, load-p99-80pct.json and load-summary.json, and the
#    whole log as load-test.txt; the teardown saves the Compose logs and runs scripts/pii-scan.sh.
# Close VS Code first on the 4 GB laptop.
set -euo pipefail
cd "$(dirname "$0")/.."

OUTDIR=${1:-docs/results}
RATES=${LOAD_RATES:-"25 50 100 200 400"}
STEP=${LOAD_STEP_SECONDS:-600}
DEV=deploy/compose/.dev
COMPOSE=(docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core)
# shellcheck source=lib/compose-stack.sh
. scripts/lib/compose-stack.sh

mkdir -p "$OUTDIR"
OUTDIR=$(cd "$OUTDIR" && pwd)
exec > >(tee "$OUTDIR/load-test.txt") 2>&1
stack_header scripts/load-test.sh "Compose profile core plus Prometheus; LOAD_RATES=\"$RATES\" LOAD_STEP_SECONDS=$STEP"
echo "# containers: driftline (every role in one JVM, -Xms256m -Xmx256m SerialGC, 512 MB), Kafka 640 MB, PostgreSQL 256 MB, sandbox 256 MB, Prometheus 160 MB; generator -Xmx192m"

stack_build :bench:installDist
BOOTSTRAP_DAYS_BACK=-2 stack_secrets 12
work=$(mktemp -d)
vmstat_pid=""
stats_pid=""
# On any exit: stop vmstat, keep its record, then the usual teardown (Compose logs, PII scan, stack removed).
on_exit() {
    local status=$?
    set +e   # under errexit, restoring a failed status below would abort this handler before the teardown
    [ -n "$vmstat_pid" ] && kill "$vmstat_pid" 2> /dev/null
    [ -n "$stats_pid" ] && kill "$stats_pid" 2> /dev/null
    cp "$work/vmstat.txt" "$DEV/load-vmstat.txt" 2> /dev/null || true
    cp "$work/dockerstats.txt" "$DEV/load-dockerstats.txt" 2> /dev/null || true
    rm -rf "$work"
    # Prometheus comes from the `obs` profile, which the core teardown does not know about
    docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile obs rm -sfv prometheus \
        > /dev/null 2>&1
    (exit "$status")
    stack_teardown
}
trap on_exit EXIT
stack_up
docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core --profile obs up -d prometheus
# D98: Prometheus asks for a password, which load-evaluate.py reads from PROMETHEUS_AUTH
PROMETHEUS_AUTH="prometheus:$(cat "$DEV/secrets/grafana/prometheus-password")"
export PROMETHEUS_AUTH
for _ in $(seq 1 30); do
    curl -fsS -u "$PROMETHEUS_AUTH" http://127.0.0.1:9090/-/ready > /dev/null 2>&1 && break
    sleep 2
done
curl -fsS -u "$PROMETHEUS_AUTH" http://127.0.0.1:9090/-/ready > /dev/null

vmstat -n 1 | awk '{print $0, systime(); fflush()}' > "$work/vmstat.txt" &
vmstat_pid=$!
# one line per container per sample: epoch, name, CPU (100% is one core), memory; each sample takes docker about 2 s
while true; do
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2> /dev/null \
        | awk -v t="$(date +%s)" '{print t, $0}'
    sleep 3
done > "$work/dockerstats.txt" &
stats_pid=$!

step() {
    local rate=$1 label=$2 out=$3 start end
    start=$(date +%s)
    echo "load: step $label at $rate events/s for $STEP s"
    JAVA_OPTS="$(stack_tls_java)" bench/build/install/bench/bin/bench --url https://127.0.0.1:8080 \
        --bootstrap "$DEV/bootstrap.json" \
        --rate "$rate" --seconds "$STEP" --label "$label" --out "$work/$label.json"
    end=$(date +%s)
    sleep 20   # two Prometheus scrapes past the step's end
    python3 scripts/load-evaluate.py "$work/$label.json" "$end" "$work/vmstat.txt" "$start" "$out" \
        http://127.0.0.1:9090 "$work/dockerstats.txt"
}

best=0
steps_json=()
stopped_by=all_rates_sustained
for rate in $RATES; do
    status=0
    step "$rate" "s$rate" "$OUTDIR/load-step-$rate.json" || status=$?
    if [ "$status" = 0 ]; then
        best=$rate
        steps_json+=("{\"rate\":$rate,\"result\":\"sustained\"}")
    elif [ "$status" = 2 ]; then
        steps_json+=("{\"rate\":$rate,\"result\":\"invalid\"}")
        stopped_by=invalid_step
        break
    else
        steps_json+=("{\"rate\":$rate,\"result\":\"not_sustained\"}")
        stopped_by=not_sustained
        break
    fi
done
if [ "$best" = 0 ]; then
    echo "load: no step was sustained" >&2
    exit 1
fi
echo "load: highest sustained offered rate $best events/s"
half=$((best / 2))
four_fifths=$((best * 4 / 5))
step "$half" "p50pct" "$OUTDIR/load-p99-50pct.json" || true
step "$four_fifths" "p80pct" "$OUTDIR/load-p99-80pct.json" || true

python3 -I - "$OUTDIR" "$best" "$half" "$four_fifths" "$(IFS=,; echo "${steps_json[*]}")" "$stopped_by" <<'PY'
import json, sys
outdir, best, half, four_fifths, steps = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
stopped_by = sys.argv[6]
meaning = {
    "not_sustained": "the limit lies between the highest sustained rate and the next step",
    "invalid_step": "the next step was INVALID (the machine paused or swapped), so the limit was not found: the highest "
                    "sustained rate is only a lower bound",
    "all_rates_sustained": "every rate tried was sustained: the limit is above the highest rate tried",
}[stopped_by]
def p99(name):
    r = json.load(open(f"{outdir}/{name}", encoding="utf-8"))
    return {"offered_rate": r["generator"]["offered_rate"], "verdict": r["verdict"],
            "event_to_match_p50_s": r["prometheus"]["event_to_match_p50_s"],
            "event_to_match_p99_s": r["prometheus"]["event_to_match_p99_s"],
            "events_consumed_per_s": r["prometheus"]["events_consumed_per_s"]}
summary = {"highest_sustained_offered_rate": best, "steps": json.loads(f"[{steps}]"), "stopped_by": stopped_by,
           "stopped_by_means": meaning,
           "at_50pct": p99("load-p99-50pct.json"), "at_80pct": p99("load-p99-80pct.json"),
           "definition": "sustained: over the last 5 minutes of a 10-minute step, consumer lag (end offsets minus "
                         "stored offsets, D83) did not grow and the generator saw no rejection, error, skip or "
                         "unanswered request (DESIGN §17)"}
with open(f"{outdir}/load-summary.json", "w", encoding="utf-8", newline="\n") as f:
    json.dump(summary, f, indent=2, sort_keys=True)
    f.write("\n")
print(json.dumps(summary, indent=2, sort_keys=True))
PY
echo "load: done; results in $OUTDIR/load-*.json"
