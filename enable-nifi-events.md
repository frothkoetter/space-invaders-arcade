# Send game events to NiFi ListenHTTP

Game build **v20+** POSTs **snake_case** JSON with **`occurred_at`** as **epoch ms** (matches Avro / Iceberg).

## Option A — Full URL in the browser (no local server)

Host the game on **any HTTP/HTTPS origin** (GitHub Pages, object storage + CDN, reverse proxy, etc.). Open **one bookmark URL**; query params are written to `localStorage` on first load.

**Example — GitHub Pages + Cloudera projtriton ListenHTTP:**

```text
https://frothkoetter.github.io/space-invaders-arcade/index.html?nifi=1&events=1&nifi-mode=simple&nifi-url=http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

Swap the path before `?` for your deployment, e.g. `https://your-cdn.example/space-invaders/index.html?…`

| Param | Effect |
|-------|--------|
| `nifi=1` | Enable POST export → `si-nifi` |
| `events=1` | Console event log → `si-events` |
| `nifi-url` | Full ListenHTTP URL → `si-nifi-listen-url` (**http://** projtriton **:9088**, not https) |
| `nifi-mode=simple` | Default cross-origin POST (text/plain JSON body; see [nifi-browser-cors.md](./nifi-browser-cors.md)) |
| `nifi-mode=cors` | `application/json` (only if gateway sends CORS) |

Intro screen shows **NiFi POST ON** and the ingest URL when enabled.

**Not supported for export:** opening `index.html` via **`file://`** (double-click). Use a hosted origin or Option B.

## Option B — Local static server

```bash
cd /path/to/space-invaders
python3 -m http.server 8080
```

**Same NiFi settings via localhost:**

```text
http://127.0.0.1:8080/index.html?nifi=1&events=1&nifi-mode=simple&nifi-url=http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

If cross-origin POST still fails from localhost, use [serve-with-nifi-proxy.py](./serve-with-nifi-proxy.py) and point `nifi-url` at `http://127.0.0.1:8080/space-invaders` (see [nifi-browser-cors.md](./nifi-browser-cors.md)).

## Console (same page)

```javascript
localStorage.setItem('si-nifi', '1')
localStorage.setItem('si-events', '1')
localStorage.setItem('si-nifi-listen-url', 'http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders')
location.reload()
```

## Play

Start the game (**Space** / tap canvas). Events fire on kills, score, lives, level up, bunker hits, etc.

Console: `[nifi] POST JSON → …` on load; `[game-event]` groups if `events=1`.

NiFi: FlowFiles on **ListenHTTP** (batched up to 50 events every 500 ms).

## Payload shape (example)

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

## Simulate many players (load test)

Script: [simulate-players-nifi.sh](./simulate-players-nifi.sh) (calls `simulate-players-nifi.py`).

```bash
cd /path/to/space-invaders
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

## CORS

Browser on `localhost:8080` → projtriton is **cross-origin**. If console shows **`POST failed … CORS`**, allow the game origin on NiFi/gateway (see [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md)) or proxy through same-origin.

## Downstream

Iceberg ingest: [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md) · schema [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json)
