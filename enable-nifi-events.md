# Send game events to NiFi ListenHTTP

Game build **v20+** POSTs **snake_case** JSON with **`occurred_at`** as **epoch ms** (matches Avro / Iceberg).

## 1. Start the game (HTTP, not file://)

```bash
cd "/Users/frothkoetter/Library/Application Support/Cursor/AgentStores/cursor_agent_stores/bc-9119102d-2261-4866-a744-dbf18eb4147a/files/docs/space-invaders"
python3 -m http.server 8080
```

## 2. Enable NiFi in one step (bookmark this URL)

**Cloudera projtriton inbound:**

```text
http://127.0.0.1:8080/index.html?nifi=1&events=1&nifi-url=http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

- `nifi=1` — enable POST export (saved to `localStorage`)
- `events=1` — log events in DevTools console
- `nifi-url` — full ListenHTTP URL (**http://** on port **9088**, not https)

Intro screen shows **NiFi POST ON** and the URL when enabled.

## 3. Or use the console (same page)

```javascript
localStorage.setItem('si-nifi', '1')
localStorage.setItem('si-events', '1')
localStorage.setItem('si-nifi-listen-url', 'http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders')
location.reload()
```

## 4. Play

Start the game (**Space** / tap canvas). Events fire on kills, score, lives, level up, bunker hits, etc.

Console: `[nifi] POST JSON → …` on load; `[game-event]` groups if `events=1`.

NiFi: FlowFiles on **ListenHTTP** (batched up to 50 events every 500 ms).

## 5. Payload shape (example)

```json
{
  "event_id": "…",
  "event_type": "invader.killed",
  "occurred_at": 1791806400000,
  "session_id": "…",
  "player_id": "anon-…",
  "player_name": "PILOT",
  "game_version": "flag-edition-v20-init-order-fix",
  "level": 1,
  "frame": 420,
  "data_json": "{\"pointsAwarded\":10}"
}
```

Smoke test: [curl-smoke-test.sh](./curl-smoke-test.sh)

## 6. Simulate many players (load test)

Script: [simulate-players-nifi.sh](./simulate-players-nifi.sh) (calls `simulate-players-nifi.py`).

```bash
cd "/Users/frothkoetter/Library/Application Support/Cursor/AgentStores/cursor_agent_stores/bc-9119102d-2261-4866-a744-dbf18eb4147a/files/docs/space-invaders"
chmod +x simulate-players-nifi.sh   # once

# Preview payloads locally (no HTTP)
DRY_RUN=1 ./simulate-players-nifi.sh

# Continuous load (same event types as index.html; NiFi batch 500ms / 50 events)
export CONCURRENCY=16
export PLAYER_COUNT=50-100
./simulate-players-nifi.sh
# Ctrl+C once to stop (~2s); Ctrl+C again to force exit (130)
# Verify NIFI_URL printed at startup matches your ListenHTTP endpoint

# Exit on first POST failure (e.g. NiFi down)
FAIL_FAST=1 PLAYER_COUNT=10 ./simulate-players-nifi.sh

# One-shot legacy mode
RUN_ONCE=1 PLAYER_COUNT=50 ./simulate-players-nifi.sh
```

Env: `NIFI_URL`, `PLAYER_COUNT` (**10–100** or **`50-100`**), `CONCURRENCY`, `DRY_RUN=1`, optional `RUN_ONCE=1`, optional **`FAIL_FAST=1`** (stop on first POST error; connection refused means nothing is listening at `NIFI_URL`).

## 7. CORS

Browser on `localhost:8080` → projtriton is **cross-origin**. If console shows **`POST failed … CORS`**, allow the game origin on NiFi/gateway (see [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md)) or proxy through same-origin.

## 8. Downstream

Iceberg ingest: [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md) · schema [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json)
