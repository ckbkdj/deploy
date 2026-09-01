#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version="$(tr -d '\r\n' < VERSION)"
out="${1:-../newapi-risk-gateway-${version}.zip}"
rm -f "$out"
find . \
  -path './.git' -prune -o \
  -path './bin' -prune -o \
  -path './dist' -prune -o \
  -path './backups' -prune -o \
  -path './data' -prune -o \
  -path './secrets' -prune -o \
  -type f \
  ! -name '.env' \
  ! -name '.env.local' \
  ! -name '.env.remote' \
  ! -name '*.log' \
  ! -name '*.pid' \
  -print | LC_ALL=C sort | zip -q "$out" -@
echo "$out"
