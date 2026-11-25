#!/usr/bin/env bash
# A live demo of Driftline on Docker Compose: the `core` and `obs` profiles with tracing on, left running so you can
# drive it and look around (the Grafana board, Jaeger traces, the ops API). Unlike scripts/e2e.sh, nothing is torn
# down until you say so.
#
#   scripts/demo.sh up                  build, generate throwaway secrets, start the stack, print where to look
#   scripts/demo.sh tour                inject a few payments of each break type, then list the breaks they raised
#   scripts/demo.sh inject TYPE [N]     inject N payments (default 3) of one scenario, e.g. DUPLICATE_CAPTURE
#   scripts/demo.sh breaks [STATE]      list the breaks, optionally only one state (e.g. ESCALATED, RESOLVED_AUTO);
#                                       FEED_STALL breaks are counted but not listed unless --all is given
#   scripts/demo.sh show BREAK_ID       one break in full: its evidence and every state it went through
#   scripts/demo.sh status              the containers and the URLs again
#   scripts/demo.sh down                save the Compose logs to deploy/compose/.dev/compose.log and remove the stack
#
# Scenarios: DEBITED_NO_ORDER LATE_SUCCESS DUPLICATE_CAPTURE SHORT_SETTLEMENT MISSING_SETTLEMENT ORPHAN_SETTLEMENT
# NORMAL AMBIGUOUS_BATCH. Each injection takes one merchant from a fresh pool of DEMO_MERCHANTS (default 12; the
# tour takes 6). A merchant's bank feed sends no file until a scenario is injected for it, so every merchant soon has a
# FEED_STALL break (DESIGN §5.4): real, but noise in a demo, hence the small pool and the hidden rows.
#
# Needs Docker and Java 21, and about 2.8 GB of memory: close VS Code first, as for scripts/e2e.sh. The ops token is
# read from deploy/compose/.dev/bootstrap.json (git-ignored) and never printed, so the screen is safe to record.
set -euo pipefail
cd "$(dirname "$0")/.."

DEV=deploy/compose/.dev
COMPOSE=(docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core --profile obs)
TOUR_TYPES=(DEBITED_NO_ORDER LATE_SUCCESS DUPLICATE_CAPTURE SHORT_SETTLEMENT MISSING_SETTLEMENT ORPHAN_SETTLEMENT)
API=http://127.0.0.1:8080
SANDBOX=http://127.0.0.1:8085
# shellcheck source=lib/compose-stack.sh
. scripts/lib/compose-stack.sh

running() {
    [ -f "$DEV/env" ] && [ -n "$("${COMPOSE[@]}" ps --quiet driftline 2> /dev/null)" ]
}

need_running() {
    if ! running; then
        echo "demo: the stack is not running; start it with: scripts/demo.sh up" >&2
        exit 1
    fi
}

urls() {
    cat <<EOF

Driftline is running. Open these in a browser:

  Grafana board (live metrics)   http://127.0.0.1:3000      (view-only without logging in)
  Jaeger (traces)                http://127.0.0.1:16686     (service: driftline)
  Prometheus (raw metrics)       http://127.0.0.1:9090
  Driftline API                  $API   (needs a token; use the commands below)

Then, in this terminal:

  scripts/demo.sh tour                 inject one batch of every break type and list what was found
  scripts/demo.sh breaks               list the breaks and their states (run it again to watch them change)
  scripts/demo.sh show <break_id>      one break: its evidence and its history
  scripts/demo.sh down                 stop everything and delete its data

EOF
}

# GET on the ops API with the ops token (never echoed).
ops_get() {
    local token
    token=$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["ops_token"])' "$DEV/bootstrap.json")
    curl -fsS -H @- "$API$1" <<< "Authorization: Bearer $token"
}

inject() {
    local type=$1 count=${2:-3}
    curl -sS -X POST "$SANDBOX/v1/inject?type=$type&count=$count" | python3 -I -c '
import json, sys
r = json.load(sys.stdin)
if "error" in r:
    sys.exit("demo: inject " + sys.argv[1] + ": " + r["error"])
print("injected %s x %-19s merchant %s: %s webhooks, %s order events, %s settlement files; expect %s break(s)"
      % (r["count"], r["type"], r["merchant_id"], r["webhooks"], r["order_events"], r["settlement_files"],
         r["expected_count"]))' "$type"
}

breaks() {
    local all=0 query=""
    if [ "${1:-}" = --all ]; then
        all=1
        shift
    fi
    if [ -n "${1:-}" ]; then
        query="?state=${1^^}"
    fi
    ops_get "/v1/breaks$query" | python3 -I -c '
import json, sys
every = sorted(json.load(sys.stdin)["breaks"], key=lambda b: (b["detected_at"], b["break_id"]))
stalls = sum(1 for b in every if b["type"] == "FEED_STALL")
rows = every if sys.argv[1] == "1" else [b for b in every if b["type"] != "FEED_STALL"]
hidden = "" if sys.argv[1] == "1" or not stalls else (
    "; %d FEED_STALL not listed (a bank feed that sent no file; --all lists them)" % stalls)
if not rows:
    print("no breaks yet" + hidden)
    sys.exit()
line = "%-20s  %-19s %-18s %-10s %s"
print(line % ("detected (UTC)", "type", "state", "merchant", "break_id"))
for b in rows:
    print(line % (b["detected_at"][:19], b["type"], b["state"], b["merchant_id"], b["break_id"]))
counts = {}
for b in rows:
    counts[b["state"]] = counts.get(b["state"], 0) + 1
print()
print("%d breaks: %s%s" % (len(rows), ", ".join("%d %s" % (n, s) for s, n in sorted(counts.items())), hidden))' "$all"
}

case "${1:-}" in
    up)
        if running; then
            echo "demo: the stack is already running"
            urls
            exit 0
        fi
        stack_build
        stack_secrets "${DEMO_MERCHANTS:-12}" "DRIFTLINE_TRACING=true"
        trap stack_teardown EXIT   # only while starting: a failed start leaves nothing behind
        stack_up
        trap - EXIT
        urls
        ;;
    tour)
        need_running
        for type in "${TOUR_TYPES[@]}"; do
            inject "$type" 3
            sleep 2
        done
        echo
        echo "waiting 20 s for Driftline to match, detect and start resolving..."
        sleep 20
        breaks
        echo
        echo "Run 'scripts/demo.sh breaks' again in a minute or two to see the states move on."
        ;;
    inject)
        need_running
        if [ -z "${2:-}" ]; then
            echo "usage: scripts/demo.sh inject TYPE [N]" >&2
            exit 2
        fi
        inject "${2^^}" "${3:-3}"
        ;;
    breaks)
        need_running
        shift
        breaks "$@"
        ;;
    show)
        need_running
        if [ -z "${2:-}" ]; then
            echo "usage: scripts/demo.sh show BREAK_ID" >&2
            exit 2
        fi
        ops_get "/v1/breaks/$2" | python3 -I -m json.tool
        ;;
    status)
        need_running
        "${COMPOSE[@]}" ps
        urls
        ;;
    down)
        if [ ! -f "$DEV/env" ]; then
            echo "demo: nothing to stop"
            exit 0
        fi
        "${COMPOSE[@]}" logs --no-color --timestamps > "$DEV/compose.log" 2>&1 || true
        "${COMPOSE[@]}" down --volumes --remove-orphans
        echo "demo: stopped; the logs are in $DEV/compose.log"
        ;;
    *)
        sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
        exit 2
        ;;
esac
