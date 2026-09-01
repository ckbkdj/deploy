#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ENV_FILE="${ENV_FILE:-.env}"
[[ -f "$ENV_FILE" ]] && { set -a; source "$ENV_FILE"; set +a; }
BROKER="${KAFKA_BOOTSTRAP:-kafka:19092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-12}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"
DAYS="${KAFKA_RETENTION_DAYS:-180}"
if [[ "$DAYS" == "-1" ]]; then RETENTION=-1; else RETENTION=$((DAYS*24*60*60*1000)); fi
TOPICS=("${KAFKA_AUDIT_TOPIC:-risk.audit.events}" "${KAFKA_TRACE_TOPIC:-risk.request.traces}" "${KAFKA_DEADLETTER_TOPIC:-risk.deadletter}")
run_kafka(){ docker compose --env-file "$ENV_FILE" -f compose.local.yml exec -T kafka "$@"; }
for topic in "${TOPICS[@]}"; do
  run_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
  run_kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" --entity-type topics --entity-name "$topic" --alter --add-config "retention.ms=$RETENTION,cleanup.policy=delete,compression.type=producer"
done
run_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --describe
