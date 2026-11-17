#!/usr/bin/env bash
# Proves that customers' contact details stay in the vault (DESIGN §14, D77) on a running Compose stack.
#
#   scripts/pii-scan.sh COMPOSE_LOG COMPOSE_COMMAND...
#   e.g. scripts/pii-scan.sh deploy/compose/.dev/compose.log docker compose -f deploy/compose/compose.yaml \
#            --env-file deploy/compose/.dev/env --profile core
#
# The sandbox adds a fake contact to every payment webhook (FakeContacts: buyerNNNNNN@okaxis, buyerNNNNNN@example.com,
# 6000NNNNNN). This script fails if any such raw value appears in:
#   1. the Compose logs of every service (masked forms such as bu***@okaxis do not match);
#   2. the Kafka topic, read from the beginning;
#   3. any table of the database other than payment_customers, whose columns are ciphertext.
# It also fails if the vault is empty, since the scan would then prove nothing. scripts/lib/compose-stack.sh runs it
# from the teardown of scripts/e2e.sh and scripts/chaos.sh, before the stack is removed.
set -uo pipefail
cd "$(dirname "$0")/.."

LOG=$1
shift
COMPOSE=("$@")
PATTERN='buyer[0-9]{6}@(okaxis|example\.com)|(^|[^0-9A-Za-z_])6000[0-9]{6}([^0-9A-Za-z_]|$)'
failed=0

report() {
    local where=$1 file=$2
    local n
    n=$(grep -aEc "$PATTERN" "$file" || true)
    if [ "$n" -gt 0 ]; then
        echo "pii-scan: FAIL: $n lines in $where hold a raw customer contact; first, masked:" >&2
        grep -aE -m 3 "$PATTERN" "$file" | sed -E 's/buyer[0-9]{6}@/bu***@/g; s/6000[0-9]{6}/60******NN/g' >&2
        failed=1
    else
        echo "pii-scan: $where: clean"
    fi
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ ! -s "$LOG" ]; then
    echo "pii-scan: FAIL: no Compose log at $LOG" >&2
    exit 1
fi
report "the Compose logs" "$LOG"

if "${COMPOSE[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic driftline.events.v1 --from-beginning --timeout-ms 15000 \
        > "$work/kafka.txt" 2> "$work/kafka.err"; then
    :
fi
records=$(wc -l < "$work/kafka.txt")
if [ "$records" -eq 0 ]; then
    echo "pii-scan: FAIL: read no records from Kafka" >&2
    sed -n 1,5p "$work/kafka.err" >&2
    failed=1
else
    report "the Kafka topic ($records records)" "$work/kafka.txt"
fi

if "${COMPOSE[@]}" exec -T postgres pg_dump -U postgres -d driftline --data-only --exclude-table=payment_customers \
        > "$work/db.sql" 2> "$work/db.err"; then
    report "the database outside the vault" "$work/db.sql"
else
    echo "pii-scan: FAIL: pg_dump failed" >&2
    sed -n 1,5p "$work/db.err" >&2
    failed=1
fi

vault=$("${COMPOSE[@]}" exec -T postgres psql -U postgres -d driftline -tAc 'SELECT count(*) FROM payment_customers' \
    2> /dev/null | tr -d '[:space:]')
if [ -z "$vault" ] || [ "$vault" -eq 0 ]; then
    echo "pii-scan: FAIL: the vault holds no contacts (got '${vault}'), so this scan proves nothing" >&2
    failed=1
else
    echo "pii-scan: the vault holds $vault encrypted contacts"
fi

if [ "$failed" -ne 0 ]; then
    exit 1
fi
echo "pii-scan: no raw customer contact outside the vault"
