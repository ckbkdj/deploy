#!/usr/bin/env bash
set -euo pipefail
URL="${1:-${GATEWAY_URL:-http://127.0.0.1:8088}}"
echo "health:"; curl -fsS "$URL/healthz"; echo
echo "readiness:"; curl -fsS "$URL/readyz"; echo
echo "metrics sample:"; curl -fsS "$URL/metrics" | head -40
