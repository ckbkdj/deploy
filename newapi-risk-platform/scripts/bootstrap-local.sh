#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v docker >/dev/null || { echo "Docker is required" >&2; exit 1; }
docker compose version >/dev/null
if [[ ! -f .env ]]; then ./scripts/generate-env.sh .env .env.example; fi
set -a; source .env; set +a
docker compose --env-file .env -f compose.local.yml up -d --build
for i in {1..90}; do
  if curl -fsS "http://127.0.0.1:${GATEWAY_PORT:-8088}/readyz" >/dev/null; then
    echo "Risk gateway ready: http://127.0.0.1:${GATEWAY_PORT:-8088}/admin/"
    echo "ADMIN_TOKEN is stored in .env"
    exit 0
  fi
  sleep 2
done
docker compose --env-file .env -f compose.local.yml ps
docker compose --env-file .env -f compose.local.yml logs --tail=120 gateway ollama-pull kafka
echo "gateway did not become ready" >&2
exit 1
