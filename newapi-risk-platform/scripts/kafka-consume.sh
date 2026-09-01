#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
kind="${1:-audit}"
case "$kind" in
  audit) topic="${KAFKA_AUDIT_TOPIC:-risk.audit.events}" ;;
  trace|tracking) topic="${KAFKA_TRACE_TOPIC:-risk.request.traces}" ;;
  deadletter|dlq) topic="${KAFKA_DEADLETTER_TOPIC:-risk.deadletter}" ;;
  *) topic="$kind" ;;
esac

mode="${MODE:-local}"
if [[ "$mode" == local ]]; then
  env_file="${ENV_FILE:-.env}"
  [[ -f "$env_file" ]] && { set -a; source "$env_file"; set +a; }
  exec docker compose --env-file "$env_file" -f compose.local.yml exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "${KAFKA_BOOTSTRAP:-kafka:19092}" \
    --topic "$topic" --from-beginning \
    --property print.key=true --property key.separator=$'\t'
fi

: "${KAFKA_BOOTSTRAP:?set KAFKA_BOOTSTRAP for remote mode}"
: "${KAFKA_COMMAND_CONFIG:?set KAFKA_COMMAND_CONFIG for remote mode}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
exec "$KAFKA_BIN/kafka-console-consumer.sh" \
  --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --consumer.config "$KAFKA_COMMAND_CONFIG" \
  --topic "$topic" --from-beginning \
  --property print.key=true --property key.separator=$'\t'
