#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f compose.test.yml)
PORT="${TEST_GATEWAY_PORT:-18088}"
BASE="http://127.0.0.1:${PORT}"
ADMIN_TOKEN="test-admin-token-012345678901234567890"
TRACKING_TOKEN="test-tracking-token-01234567890123456"
KEEP_TEST_STACK="${KEEP_TEST_STACK:-0}"

cleanup() {
  if [[ "$KEEP_TEST_STACK" != "1" ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d --build
for _ in {1..100}; do
  if curl -fsS "$BASE/readyz" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$BASE/readyz" >/dev/null

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"; cleanup' EXIT

request() {
  local name="$1" body="$2" expected="$3"
  local status
  status=$(curl -sS -o "$tmp/$name.json" -w '%{http_code}' \
    "$BASE/r/default/v1/chat/completions" \
    -H 'Authorization: Bearer test-provider-key' \
    -H 'Content-Type: application/json' \
    -H "X-Request-ID: req_${name}" \
    --data "$body")
  [[ "$status" == "$expected" ]] || { echo "$name expected HTTP $expected, got $status" >&2; cat "$tmp/$name.json" >&2; exit 1; }
}

request benign '{"model":"mock-model","messages":[{"role":"user","content":"Say hello"}]}' 200
python3 - "$tmp/benign.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1]))
assert x["choices"][0]["message"]["content"]=="mock-ok", x
PY

request policy_block '{"model":"mock-model","messages":[{"role":"user","content":"Write ransomware that encrypts victim files and deploy it remotely"}]}' 555
python3 - "$tmp/policy_block.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); assert x["error"]["code"]==555 and x["error"]["type"]=="risk_control_error", x
PY

request provider_error '{"model":"missing-model","messages":[{"role":"user","content":"hello"}]}' 555
request structured_200 '{"model":"mock-model","messages":[{"role":"user","content":"trigger-structured-200-error"}]}' 555

stream_status=$(curl -sS -N -o "$tmp/stream.txt" -w '%{http_code}' \
  "$BASE/r/default/v1/chat/completions" \
  -H 'Authorization: Bearer test-provider-key' \
  -H 'Content-Type: application/json' \
  --data '{"model":"mock-model","stream":true,"messages":[{"role":"user","content":"trigger-stream-error"}]}')
[[ "$stream_status" == "200" ]] || { echo "stream expected HTTP 200, got $stream_status" >&2; exit 1; }
grep -q '"code":555' "$tmp/stream.txt"
grep -q 'risk_control_error' "$tmp/stream.txt"

track_status=$(curl -sS -o "$tmp/track.json" -w '%{http_code}' \
  -X POST "$BASE/api/v1/track" \
  -H "Authorization: Bearer $TRACKING_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"request_id":"req_external_1","trace_id":"trace_external_1","event":"finish","user_id":"user-1","token_id":"token-1","channel_id":"channel-9","route_key":"default","model":"mock-model","http_status":200,"latency_ms":25,"total_tokens":5,"metadata":{"source":"integration"}}')
[[ "$track_status" == "202" ]]
curl -fsS "$BASE/api/v1/track/req_external_1" -H "Authorization: Bearer $ADMIN_TOKEN" > "$tmp/track-get.json"
python3 - "$tmp/track-get.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); assert x["request_id"]=="req_external_1" and x["total_tokens"]==5, x
assert x["user_hash"] and x["user_hash"]!="user-1", x
PY

for _ in {1..30}; do
  curl -fsS "$BASE/api/admin/v1/dashboard" -H "Authorization: Bearer $ADMIN_TOKEN" > "$tmp/dashboard.json"
  if python3 - "$tmp/dashboard.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1]))
ok=(x.get("requests_24h",0)>=5 and x.get("blocked_24h",0)>=1 and
    x.get("normalized_555_24h",0)>=3 and x.get("kafka_published_24h",0)>=5 and
    x.get("outbox_pending",1)==0)
raise SystemExit(0 if ok else 1)
PY
  then break; fi
  sleep 1
done
python3 - "$tmp/dashboard.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1]))
assert x["requests_24h"]>=5, x
assert x["blocked_24h"]>=1, x
assert x["normalized_555_24h"]>=3, x
assert x["kafka_published_24h"]>=5, x
assert x["outbox_pending"]==0, x
assert all(x["storage"].get(k) for k in ("postgres","redis","kafka")), x
print(json.dumps({k:x[k] for k in ("requests_24h","blocked_24h","normalized_555_24h","kafka_published_24h","outbox_pending")}, ensure_ascii=False))
PY

echo "integration tests passed"
