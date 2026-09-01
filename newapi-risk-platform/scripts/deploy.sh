#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  cat <<'USAGE'
Usage: ./scripts/deploy.sh COMMAND

Commands:
  local-up           Generate .env when absent and start the local full stack
  local-down         Stop the local full stack
  remote-up          Start gateway-only remote-infrastructure mode
  remote-down        Stop gateway-only mode
  observability-up   Start Prometheus, Grafana and read-only Kafka UI
  observability-down Stop the observability stack
  status             Show all known Compose stacks
  logs               Follow gateway logs (MODE=local|remote, default local)
  test               Run deterministic unit/static checks and Docker E2E
  doctor             Validate local or remote deployment inputs (MODE=local|remote)
USAGE
}

command="${1:-}"
case "$command" in
  local-up)
    exec ./scripts/bootstrap-local.sh
    ;;
  local-down)
    [[ -f .env ]] || { echo ".env not found" >&2; exit 1; }
    exec docker compose --env-file .env -f compose.local.yml down
    ;;
  remote-up)
    exec ./scripts/bootstrap-remote.sh
    ;;
  remote-down)
    env_file="${ENV_FILE:-.env.remote}"
    [[ -f "$env_file" ]] || { echo "$env_file not found" >&2; exit 1; }
    exec docker compose --env-file "$env_file" -f compose.remote.yml down
    ;;
  observability-up)
    [[ -f .env ]] || { echo ".env not found; run local-up first" >&2; exit 1; }
    exec docker compose --env-file .env -f compose.observability.yml up -d
    ;;
  observability-down)
    [[ -f .env ]] || { echo ".env not found" >&2; exit 1; }
    exec docker compose --env-file .env -f compose.observability.yml down
    ;;
  status)
    if [[ -f .env ]]; then
      docker compose --env-file .env -f compose.local.yml ps || true
      docker compose --env-file .env -f compose.observability.yml ps || true
    fi
    if [[ -f "${ENV_FILE:-.env.remote}" ]]; then
      docker compose --env-file "${ENV_FILE:-.env.remote}" -f compose.remote.yml ps || true
    fi
    ;;
  logs)
    if [[ "${MODE:-local}" == "remote" ]]; then
      exec docker compose --env-file "${ENV_FILE:-.env.remote}" -f compose.remote.yml logs -f --tail=200 gateway
    fi
    exec docker compose --env-file .env -f compose.local.yml logs -f --tail=200 gateway
    ;;
  test)
    make check
    go test ./...
    exec ./scripts/integration-test.sh
    ;;
  doctor)
    exec ./scripts/doctor.sh "${MODE:-local}"
    ;;
  *)
    usage
    [[ -z "$command" ]] || exit 1
    ;;
esac
