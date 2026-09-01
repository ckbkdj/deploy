#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${1:-local}"

failures=0
ok() { printf '[OK] %s\n' "$*"; }
fail() { printf '[FAIL] %s\n' "$*" >&2; failures=$((failures+1)); }
warn() { printf '[WARN] %s\n' "$*" >&2; }
need() { command -v "$1" >/dev/null 2>&1 && ok "$1 found" || fail "$1 is required"; }

need docker
if command -v docker >/dev/null 2>&1; then
  docker compose version >/dev/null 2>&1 && ok 'Docker Compose v2 available' || fail 'Docker Compose v2 unavailable'
fi
need curl
need awk

case "$mode" in
  local)
    env_file="${ENV_FILE:-.env}"
    compose_file=compose.local.yml
    ;;
  remote)
    env_file="${ENV_FILE:-.env.remote}"
    compose_file=compose.remote.yml
    ;;
  *) fail 'mode must be local or remote'; env_file=.env; compose_file=compose.local.yml ;;
esac

if [[ ! -f "$env_file" ]]; then
  fail "$env_file does not exist"
else
  ok "$env_file exists"
  permissions=$(stat -c '%a' "$env_file" 2>/dev/null || stat -f '%Lp' "$env_file" 2>/dev/null || echo unknown)
  [[ "$permissions" == "600" ]] && ok "$env_file mode is 600" || warn "$env_file mode is $permissions; chmod 600 is recommended"
  if grep -Eq '(^|=)(CHANGE_ME|URL_ENCODED_PASSWORD|REDACTED)($|_)' "$env_file"; then
    fail "$env_file still contains placeholders"
  else
    ok 'no known placeholders found'
  fi
  for key in DATABASE_URL ADMIN_TOKEN TRACKING_TOKEN HASH_SECRET MASTER_KEY; do
    grep -Eq "^${key}=.+" "$env_file" && ok "$key is set" || fail "$key is missing"
  done
  if [[ "$mode" == local ]]; then
    for key in POSTGRES_PASSWORD REDIS_PASSWORD; do
      grep -Eq "^${key}=.+" "$env_file" && ok "$key is set" || fail "$key is missing"
    done
  else
    for key in REDIS_URL KAFKA_BROKERS; do
      grep -Eq "^${key}=.+" "$env_file" && ok "$key is set" || fail "$key is missing"
    done
    if grep -Eq '^DATABASE_URL=.*sslmode=disable' "$env_file"; then
      warn 'remote DATABASE_URL disables TLS'
    fi
    if grep -Eq '^KAFKA_TLS_INSECURE_SKIP_VERIFY=true' "$env_file"; then
      warn 'Kafka TLS certificate verification is disabled'
    fi
  fi
fi

if command -v docker >/dev/null 2>&1 && [[ -f "$env_file" ]]; then
  if docker compose --env-file "$env_file" -f "$compose_file" config --quiet >/dev/null; then
    ok "$compose_file renders successfully"
  else
    fail "$compose_file failed to render"
  fi
fi

if [[ "$failures" -gt 0 ]]; then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo 'doctor checks passed'
