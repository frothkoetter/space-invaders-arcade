#!/usr/bin/env bash
# Space Invaders — NiFi smoke test (occurred_at = epoch ms, no ISO string)
set -euo pipefail

URL="${SI_SMOKE_URL:-http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders}"
# Override: SI_OCCURRED_AT_MS=1791806400000 bash curl-smoke-test.sh
OCCURRED_AT_MS="${SI_OCCURRED_AT_MS:-$(($(date +%s) * 1000))}"

curl -sS -w "\nHTTP %{http_code}\n occurred_at_ms=${OCCURRED_AT_MS}\n" -X POST "$URL" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d "{\"event_id\":\"00000000-0000-4000-8000-000000000001\",\"event_type\":\"invader.killed\",\"occurred_at\":${OCCURRED_AT_MS},\"session_id\":\"curl-smoke\",\"player_id\":\"anon-curl\",\"player_name\":\"PILOT\",\"game_version\":\"flag-edition-v17-epoch-millis-ts\",\"level\":1,\"frame\":100,\"data_json\":\"{\\\"pointsAwarded\\\":10}\"}"
