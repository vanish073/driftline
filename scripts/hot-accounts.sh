#!/usr/bin/env bash
# The hot-account benchmark (DESIGN §7.5, §17): ROW_LOCK vs APPEND_SNAPSHOT x writers {1, 4, 16, 64} on one merchant's
# hot accounts, in a fresh PostgreSQL 17 container with production's roles (the benchmark posts as the DML login).
#
#   scripts/hot-accounts.sh [out-dir]      default docs/results; HOT_SECONDS (default 20) per run, HOT_WARMUP (5)
#
# Writes hot-accounts.csv (the numbers), hot-accounts.txt (how they were taken: commit, machine, PostgreSQL settings,
# swap) and hot-accounts.svg (the chart). A run during which the machine swapped in for 5 seconds in a row is rejected,
# since its latencies would measure the swap (DESIGN §15.1). Close VS Code first on the 4 GB laptop.
set -euo pipefail
cd "$(dirname "$0")/.."

OUTDIR=${1:-docs/results}
SECONDS_PER_RUN=${HOT_SECONDS:-20}
WARMUP=${HOT_WARMUP:-5}
POSTGRES=postgres:17@sha256:846a7ddaa67a4b354ae1d21087b683901aa6113a4ed626d3787cfbf6c85e3e07
PORT=55432
name=driftline-hot-$$
admin_password=$(openssl rand -hex 16)
app_password=$(openssl rand -hex 16)
work=$(mktemp -d)
vmstat_pid=""
cleanup() {
    [ -n "$vmstat_pid" ] && kill "$vmstat_pid" 2> /dev/null || true
    docker rm -f "$name" > /dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

mkdir -p "$OUTDIR"
OUTDIR=$(cd "$OUTDIR" && pwd)
docker run -d --name "$name" --memory 768m -p "127.0.0.1:$PORT:5432" \
    -e POSTGRES_DB=driftline -e POSTGRES_PASSWORD="$admin_password" -e DRIFTLINE_DB_PASSWORD="$app_password" \
    -v "$PWD/deploy/compose/postgres-init.sh:/docker-entrypoint-initdb.d/01-roles.sh:ro" \
    "$POSTGRES" -c shared_buffers=128MB -c max_connections=100 > /dev/null
# the image restarts the server once after running the init script: wait until the DML login can connect
for _ in $(seq 1 60); do
    if docker exec -e PGPASSWORD="$app_password" "$name" psql -h 127.0.0.1 -U driftline -d driftline -tAc 'select 1' \
            > /dev/null 2>&1; then
        break
    fi
    sleep 1
done
pg() {
    docker exec "$name" psql -U postgres -d driftline -tAc "$1"
}

{
    echo "# scripts/hot-accounts.sh, $(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD)$(git diff --quiet || echo '+uncommitted')"
    echo "# host: $(uname -sr), $(nproc) cores, $(free -m | awk '/Mem:/ {print $2}') MB RAM, swap used at start $(free -m | awk '/Swap:/ {print $3}') MB"
    echo "# java: $(java -version 2>&1 | head -1)"
    echo "# postgres: $(pg 'select version()' | cut -d, -f1); container memory 768 MB; shared_buffers=$(pg 'show shared_buffers'), fsync=$(pg 'show fsync'), synchronous_commit=$(pg 'show synchronous_commit'), isolation READ COMMITTED"
    echo "# runs: ${WARMUP} s warm-up then ${SECONDS_PER_RUN} s measured, per strategy and writer count; one merchant, distinct captures; a reader every 50 ms; APPEND_SNAPSHOT snapshots every 1 s"
} > "$OUTDIR/hot-accounts.txt"
cat "$OUTDIR/hot-accounts.txt"

vmstat -n 1 > "$work/vmstat.txt" &
vmstat_pid=$!
./gradlew -q :bench:hotAccounts --args="--jdbc jdbc:postgresql://127.0.0.1:$PORT/driftline \
    --admin-password $admin_password --password $app_password --warmup $WARMUP --seconds $SECONDS_PER_RUN \
    --out $OUTDIR/hot-accounts.csv" 2>&1 | grep -v 'Sharing is only supported' | tee -a "$OUTDIR/hot-accounts.txt"
kill "$vmstat_pid"
vmstat_pid=""

# swap-in (vmstat column si, KB/s): reject on 5 consecutive seconds above zero
verdict=$(awk 'NR > 2 { if ($7 > 0) run++; else run = 0; if (run > max) max = run; total += $7 } END {
    printf "%s %d %d", (max >= 5 ? "REJECTED" : "ok"), max, total }' "$work/vmstat.txt")
read -r status longest total <<< "$verdict"
echo "# swap-in during the runs: longest streak ${longest} s, total ${total} KB; verdict: ${status}" \
    | tee -a "$OUTDIR/hot-accounts.txt"
if [ "$status" = REJECTED ]; then
    echo "hot-accounts: the machine swapped during the runs; the numbers are not kept" >&2
    rm -f "$OUTDIR/hot-accounts.csv"
    exit 1
fi
python3 scripts/hot-accounts-chart.py "$OUTDIR/hot-accounts.csv" "$OUTDIR/hot-accounts.svg"
echo "hot-accounts: wrote $OUTDIR/hot-accounts.csv, .txt and .svg"
