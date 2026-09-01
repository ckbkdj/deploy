#!/usr/bin/env sh
set -eu
OUT="${1:-.env}"
TEMPLATE="${2:-.env.example}"
[ -f "$TEMPLATE" ] || { echo "missing $TEMPLATE" >&2; exit 1; }
if [ -e "$OUT" ] && [ "${FORCE:-0}" != "1" ]; then
  echo "$OUT already exists; set FORCE=1 to overwrite" >&2
  exit 1
fi
rand_hex() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
  else
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
  fi
}
PG_PASS=$(rand_hex)
REDIS_PASS=$(rand_hex)
ADMIN=$(rand_hex)
TRACK=$(rand_hex)
HASH=$(rand_hex)
MASTER=$(rand_hex)
GRAFANA=$(rand_hex)
TMP="${OUT}.tmp.$$"
trap 'rm -f "$TMP"' EXIT HUP INT TERM
awk \
  -v pg="$PG_PASS" -v redis="$REDIS_PASS" -v admin="$ADMIN" \
  -v track="$TRACK" -v hash="$HASH" -v master="$MASTER" -v grafana="$GRAFANA" '
  /^POSTGRES_PASSWORD=/ { print "POSTGRES_PASSWORD=" pg; next }
  /^DATABASE_URL=postgres:\/\/risk:/ { print "DATABASE_URL=postgres://risk:" pg "@postgres:5432/risk?sslmode=disable"; next }
  /^REDIS_PASSWORD=/ { print "REDIS_PASSWORD=" redis; next }
  /^REDIS_URL=redis:\/\/:/ { print "REDIS_URL=redis://:" redis "@redis:6379/0"; next }
  /^ADMIN_TOKEN=/ { print "ADMIN_TOKEN=" admin; next }
  /^TRACKING_TOKEN=/ { print "TRACKING_TOKEN=" track; next }
  /^HASH_SECRET=/ { print "HASH_SECRET=" hash; next }
  /^MASTER_KEY=/ { print "MASTER_KEY=" master; next }
  /^GRAFANA_ADMIN_PASSWORD=/ { print "GRAFANA_ADMIN_PASSWORD=" grafana; seen_grafana=1; next }
  { print }
  END { if (!seen_grafana) print "GRAFANA_ADMIN_PASSWORD=" grafana }
' "$TEMPLATE" > "$TMP"
mv "$TMP" "$OUT"
trap - EXIT HUP INT TERM
chmod 600 "$OUT"
echo "generated $OUT (mode 600)"
