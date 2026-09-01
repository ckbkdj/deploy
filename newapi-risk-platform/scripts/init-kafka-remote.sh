#!/usr/bin/env bash
set -euo pipefail
: "${KAFKA_BOOTSTRAP:?set KAFKA_BOOTSTRAP}"
: "${KAFKA_COMMAND_CONFIG:?set KAFKA_COMMAND_CONFIG to a Kafka client.properties file}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-24}"; REPLICATION="${KAFKA_REPLICATION_FACTOR:-3}"; DAYS="${KAFKA_RETENTION_DAYS:-365}"
if [[ "$DAYS" == "-1" ]]; then RETENTION=-1; else RETENTION=$((DAYS*24*60*60*1000)); fi
for topic in "${KAFKA_AUDIT_TOPIC:-risk.audit.events}" "${KAFKA_TRACE_TOPIC:-risk.request.traces}" "${KAFKA_DEADLETTER_TOPIC:-risk.deadletter}"; do
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --command-config "$KAFKA_COMMAND_CONFIG" --create --if-not-exists --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --command-config "$KAFKA_COMMAND_CONFIG" --entity-type topics --entity-name "$topic" --alter --add-config "retention.ms=$RETENTION,cleanup.policy=delete,compression.type=producer"
done
