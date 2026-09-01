#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${TARGET_URL:=http://host.docker.internal:8088/r/default/v1/chat/completions}"
: "${UPSTREAM_API_KEY:=test-key}"
docker run --rm --add-host=host.docker.internal:host-gateway -e TARGET_URL -e UPSTREAM_API_KEY -e VUS="${VUS:-20}" -e DURATION="${DURATION:-30s}" -v "$PWD/testdata:/scripts:ro" grafana/k6:1.8.1 run /scripts/k6.js
