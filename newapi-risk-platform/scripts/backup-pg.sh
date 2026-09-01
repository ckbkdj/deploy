#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."; mkdir -p backups
[[ -f .env ]] && { set -a; source .env; set +a; }
OUT="${1:-backups/risk-$(date -u +%Y%m%dT%H%M%SZ).sql.gz}"
docker compose --env-file .env -f compose.local.yml exec -T postgres pg_dump -U "${POSTGRES_USER:-risk}" -d "${POSTGRES_DB:-risk}" --no-owner --no-privileges | gzip -9 > "$OUT"
echo "$OUT"
