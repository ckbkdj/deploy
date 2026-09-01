#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] && { set -a; source .env; set +a; }
MODEL="${1:-${AUDIT_MODEL_NAME:-qwen3.5:4b}}"
docker compose --env-file .env -f compose.local.yml exec ollama ollama pull "$MODEL"
