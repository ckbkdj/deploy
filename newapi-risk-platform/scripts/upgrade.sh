#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${1:-remote}"
case "$mode" in
  local)
    [[ -f .env ]] || { echo '.env not found' >&2; exit 1; }
    docker compose --env-file .env -f compose.local.yml build --pull gateway
    docker compose --env-file .env -f compose.local.yml up -d --no-deps gateway
    url="http://127.0.0.1:${GATEWAY_PORT:-8088}"
    ;;
  remote)
    env_file="${ENV_FILE:-.env.remote}"
    [[ -f "$env_file" ]] || { echo "$env_file not found" >&2; exit 1; }
    set -a; source "$env_file"; set +a
    docker compose --env-file "$env_file" -f compose.remote.yml pull gateway || true
    docker compose --env-file "$env_file" -f compose.remote.yml build --pull gateway
    docker compose --env-file "$env_file" -f compose.remote.yml up -d --no-deps gateway
    url="http://127.0.0.1:${GATEWAY_PORT:-8088}"
    ;;
  *) echo 'usage: upgrade.sh local|remote' >&2; exit 1 ;;
esac
for _ in {1..60}; do
  if curl -fsS "$url/readyz" >/dev/null 2>&1; then
    echo "upgrade ready: $url"
    exit 0
  fi
  sleep 2
done
echo 'upgraded container did not become ready' >&2
exit 1
