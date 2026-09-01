#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."; FILE="${1:?usage: $0 backup.sql.gz}"
[[ -f .env ]] && { set -a; source .env; set +a; }
echo "This restores into ${POSTGRES_DB:-risk}. Existing rows may conflict. Type RESTORE to continue:"
read -r answer; [[ "$answer" == RESTORE ]] || exit 1
gzip -dc "$FILE" | docker compose --env-file .env -f compose.local.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-risk}" -d "${POSTGRES_DB:-risk}"
