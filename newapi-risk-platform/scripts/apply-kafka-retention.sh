#!/usr/bin/env bash
set -euo pipefail
DAYS="${1:?usage: $0 DAYS [GATEWAY_URL] [ADMIN_TOKEN]}"
URL="${2:-${GATEWAY_URL:-http://127.0.0.1:8088}}"
TOKEN="${3:-${ADMIN_TOKEN:-}}"
if [[ -z "$TOKEN" && -f .env ]]; then TOKEN=$(grep '^ADMIN_TOKEN=' .env | cut -d= -f2-); fi
[[ -n "$TOKEN" ]] || { echo "ADMIN_TOKEN required" >&2; exit 1; }
curl -fsS -X POST "$URL/api/admin/v1/kafka/retention/apply" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data "{\"days\":$DAYS}"; echo
