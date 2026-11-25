#!/usr/bin/env bash
# The chaos run (Shift 8; DESIGN §9.4, D65): Driftline in Docker Compose with the matcher and the dispatcher in
# containers of their own, killed with SIGKILL at random moments while the sandbox injects traffic, then
# `driftline verify` checks the books against the world.
#
#   scripts/chaos.sh [results-file]   CHAOS_ROUNDS (default 2), CHAOS_SECONDS (default 180), CHAOS_SEED (random, printed)
#
# 1. Builds and generates secrets as scripts/e2e.sh does; Driftline runs ingest, the settlement worker and the resolver
#    in one container, the matcher and the dispatcher in their own (profiles `core` and `chaos`).
# 2. Once the matcher and the dispatcher are healthy, injects every one-to-one scenario and NORMAL traffic,
#    CHAOS_ROUNDS times, spread over CHAOS_SECONDS, while a killer loop sends SIGKILL to the matcher and the dispatcher
#    in turn every 5 to 15 seconds and starts each again 2 to 8 seconds later, for at least CHAOS_SECONDS. The gaps come
#    from bash's RANDOM seeded with CHAOS_SEED, so it is printed and can be replayed (container timing still varies).
# 3. Starts both again, then runs `driftline verify` (profile `verify`) once; it re-checks for up to 5 minutes while
#    the system settles: ledger balanced, one refund command per payment, legal transitions, every break reviewed,
#    deadlines met, the outbox drained, no payment refunded twice by the gateway, and every refund booked once.
# 4. Prints the sandbox's counts. Exits with verify's status, or 1 if no refund was made or a role was never killed
#    (such a run proves nothing). Always saves the Compose logs to deploy/compose/.dev/compose.log and tears down.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS=${1:-}
ROUNDS=${CHAOS_ROUNDS:-2}
DURATION=${CHAOS_SECONDS:-180}
SEED=${CHAOS_SEED:-$((RANDOM * 32768 + RANDOM))}
DEV=deploy/compose/.dev
COMPOSE=(docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile core --profile chaos)
TYPES=(DEBITED_NO_ORDER LATE_SUCCESS DUPLICATE_CAPTURE SHORT_SETTLEMENT MISSING_SETTLEMENT ORPHAN_SETTLEMENT NORMAL)
# shellcheck source=lib/compose-stack.sh
. scripts/lib/compose-stack.sh

if [ -n "$RESULTS" ]; then
    mkdir -p "$(dirname "$RESULTS")"
    exec > >(tee "$RESULTS") 2>&1
    stack_header scripts/chaos.sh "Compose profiles core and chaos; CHAOS_ROUNDS=$ROUNDS CHAOS_SECONDS=$DURATION CHAOS_SEED=$SEED"
fi
echo "chaos: seed $SEED, $ROUNDS rounds of ${#TYPES[@]} injections"

stack_build
stack_secrets $((ROUNDS * ${#TYPES[@]} + 2)) "DRIFTLINE_CORE_ROLE=ingest,settlement-worker,resolver"
trap stack_teardown EXIT
stack_up

# Waits until a service's container reports healthy.
healthy() {
    for _ in $(seq 1 90); do
        if [ "$(docker inspect -f '{{.State.Health.Status}}' "$("${COMPOSE[@]}" ps -q "$1")" 2> /dev/null)" = healthy ]; then
            return 0
        fi
        sleep 2
    done
    echo "chaos: $1 never became healthy" >&2
    return 1
}

# The injections, spread evenly over the chaos window so the kills land during traffic.
traffic() {
    local gap=$((DURATION / (ROUNDS * ${#TYPES[@]})))
    for round in $(seq 1 "$ROUNDS"); do
        for type in "${TYPES[@]}"; do
            if ! curl -fsS --cacert "$DEV/secrets/ca.crt" -X POST "https://127.0.0.1:8085/v1/inject?type=$type&count=3" > /dev/null; then
                echo "chaos: inject $type (round $round) failed" >&2
                return 1
            fi
            echo "chaos: injected $type (round $round)"
            sleep "$gap"
        done
    done
}

healthy matcher
healthy dispatcher
RANDOM=$SEED
victims=(matcher dispatcher)
turn=$((RANDOM % 2))
declare -A kills=([matcher]=0 [dispatcher]=0)
traffic &
traffic_pid=$!
started=$SECONDS
# Kills alternate between the two, so both die several times; the gaps and downtimes come from the seed.
while kill -0 "$traffic_pid" 2> /dev/null || [ $((SECONDS - started)) -lt "$DURATION" ]; do
    sleep $((5 + RANDOM % 11))
    victim=${victims[$turn]}
    turn=$((1 - turn))
    down=$((2 + RANDOM % 7))
    echo "chaos: SIGKILL $victim at +$((SECONDS - started)) s, down ${down} s"
    "${COMPOSE[@]}" kill -s KILL "$victim" > /dev/null 2>&1 || true
    kills[$victim]=$((kills[$victim] + 1))
    sleep "$down"
    "${COMPOSE[@]}" start "$victim" > /dev/null
done
wait "$traffic_pid"
"${COMPOSE[@]}" start matcher dispatcher > /dev/null
echo "chaos: traffic done; killed the matcher ${kills[matcher]} times and the dispatcher ${kills[dispatcher]} times"

status=0
docker compose -f deploy/compose/compose.yaml --env-file "$DEV/env" --profile verify run --rm verify || status=$?
stats=$(curl -fsS --cacert "$DEV/secrets/ca.crt" https://127.0.0.1:8085/v1/stats)
echo "chaos: sandbox $stats"
refunds=$(python3 -I -c 'import json, sys; print(json.loads(sys.argv[1])["refunds"])' "$stats")
if [ "$status" -eq 0 ] && { [ "$refunds" -eq 0 ] || [ "${kills[matcher]}" -eq 0 ] || [ "${kills[dispatcher]}" -eq 0 ]; }; then
    echo "chaos: no refund, or a role never killed: the run proves nothing" >&2
    status=1
fi
echo "chaos: seed $SEED, matcher killed ${kills[matcher]} times, dispatcher ${kills[dispatcher]} times, $refunds refunds, verify exit status $status"
exit "$status"
