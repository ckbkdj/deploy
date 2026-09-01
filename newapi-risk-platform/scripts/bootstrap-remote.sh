#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ENV_FILE="${ENV_FILE:-.env.remote}"
[[ -f "$ENV_FILE" ]] || { cp .env.remote.example "$ENV_FILE"; chmod 600 "$ENV_FILE"; echo "Created $ENV_FILE; fill real remote endpoints and secrets first." >&2; exit 1; }
for key in DATABASE_URL REDIS_URL KAFKA_BROKERS ADMIN_TOKEN TRACKING_TOKEN HASH_SECRET MASTER_KEY; do
  value=$(grep -E "^${key}=" "$ENV_FILE" | tail -1 | cut -d= -f2- || true)
  [[ -n "$value" && "$value" != *CHANGE_ME* ]] || { echo "$key is missing or still a placeholder in $ENV_FILE" >&2; exit 1; }
done
docker compose --env-file "$ENV_FILE" -f compose.remote.yml up -d --build
set -a; source "$ENV_FILE"; set +a
for i in {1..60}; do curl -fsS "http://127.0.0.1:${GATEWAY_PORT:-8088}/readyz" && echo && exit 0; sleep 2; done
docker compose --env-file "$ENV_FILE" -f compose.remote.yml logs --tail=200 gateway
exit 1
