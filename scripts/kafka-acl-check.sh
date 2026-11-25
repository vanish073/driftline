#!/usr/bin/env bash
# Kafka's permissions hold (D101), checked from inside the broker's container with Driftline's own login:
#   allowed: reading its topic's offsets;
#   refused: creating a topic, and reading its topic through a consumer group other than its own.
#   scripts/kafka-acl-check.sh EXEC_PREFIX...   e.g. docker compose -f ... exec -T kafka   or   kubectl exec kafka-0 --
set -uo pipefail
exec_prefix=("$@")
inside() {
    "${exec_prefix[@]}" bash -c "KAFKA_HEAP_OPTS=-Xmx64m KAFKA_OPTS= $1" 2>&1
}
P=/etc/kafka/secrets/driftline-client.properties
failed=0
# the offsets of every partition, as the backlog probe reads them (Describe on the topic; kafka-topics --describe would
# also read the topic's configuration, which the login does not need and may not do)
if [ "$(inside "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --command-config $P --topic driftline.events.v1" \
        | grep -c '^driftline.events.v1:')" = 12 ]; then
    echo "kafka-acl-check: the driftline login reads its topic's offsets (12 partitions)"
else
    echo "kafka-acl-check: FAIL: the driftline login cannot read its own topic's offsets" >&2
    failed=1
fi
out=$(inside "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --command-config $P --create --topic acl-probe --partitions 1 --replication-factor 1")
if grep -qi 'authoriz' <<< "$out"; then
    echo "kafka-acl-check: the driftline login may not create a topic"
else
    echo "kafka-acl-check: FAIL: the driftline login created a topic, or failed for another reason: $out" >&2
    failed=1
fi
out=$(inside "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --consumer.config $P --topic driftline.events.v1 --group acl-probe-intruder --max-messages 1 --timeout-ms 15000")
if grep -qi 'not authorized to access group\|GroupAuthorization' <<< "$out"; then
    echo "kafka-acl-check: the driftline login may not use another consumer group"
else
    echo "kafka-acl-check: FAIL: another consumer group was allowed, or failed for another reason: $out" >&2
    failed=1
fi
exit "$failed"
