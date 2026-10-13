#!/usr/bin/env bash
# Continuous sim: 10–100 players play full game loops and POST to NiFi until Ctrl+C.
#
# Payload: same snake_case records as index.html publishGameEvent / curl-smoke-test.sh
# (occurred_at epoch ms, data_json string). Batch wrapper matches nifiPostBody:
#   1 event → single JSON object; 2+ → JSON array.
#
# Content-Type: default text/plain;charset=UTF-8 (browser simple mode). NiFi ListenHTTP
# accepts the JSON bytes either way; use NIFI_CONTENT_TYPE=application/json for curl parity.
#
# Environment:
#   NIFI_URL            default projtriton :9088 /space-invaders
#   PLAYER_COUNT        default 25; integer 10–100 or range 50-100 (random in range)
#   CONCURRENCY         parallel HTTP POST workers (default 8)
#   RUN_ONCE=1          legacy: one batch per player then exit
#   DRY_RUN=1           print sample bodies, no network
#   FAIL_FAST=1         exit on first POST failure
#   NIFI_CONTENT_TYPE   override POST header
#
# Mac Agent Store path (example):
#   cd "/Users/frothkoetter/Library/Application Support/Cursor/AgentStores/cursor_agent_stores/bc-9119102d-2261-4866-a744-dbf18eb4147a/files/docs/space-invaders"
# Run from this directory with ./ (not on PATH unless you add it):
#   export CONCURRENCY=16
#   export PLAYER_COUNT=50-100
#   ./simulate-players-nifi.sh      # runs until Ctrl+C
#
# Examples:
#   DRY_RUN=1 ./simulate-players-nifi.sh
#   PLAYER_COUNT=50 ./simulate-players-nifi.sh
#   PLAYER_COUNT=50-100 CONCURRENCY=16 ./simulate-players-nifi.sh
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$DIR/simulate-players-nifi.py" "$@"
