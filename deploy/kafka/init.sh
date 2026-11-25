#!/bin/bash
# Kafka's one-shot setup (DESIGN §3.3, D101), run by Compose's kafka-init service and the kind overlay's kafka-init Job
# with the admin login: the topic (12 partitions), then what Driftline's own login may do. The broker denies anything
# not granted (StandardAuthorizer; only the broker and the admin are super users), so the driftline login may read,
# write and describe its one topic and use its consumer group, and nothing else: no other topic, no other group, no
# topic creation or deletion, no ACL changes. Both steps can be re-run harmlessly.
set -euo pipefail
B=kafka:9092
C=/etc/kafka/secrets/client.properties
T=driftline.events.v1
G=driftline-matcher
for attempt in $(seq 1 30); do
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$B" --command-config "$C" --create --if-not-exists \
            --topic "$T" --partitions 12 --replication-factor 1; then
        break
    fi
    [ "$attempt" = 30 ] && exit 1
    sleep 2
done
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$B" --command-config "$C" --add --allow-principal User:driftline \
    --operation Read --operation Write --operation Describe --topic "$T"
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$B" --command-config "$C" --add --allow-principal User:driftline \
    --operation Read --operation Describe --group "$G"
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$B" --command-config "$C" --list --principal User:driftline
