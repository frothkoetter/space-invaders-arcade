# Space Invaders — Flag Edition

A browser-based classic Space Invaders arcade game built with HTML5 Canvas and vanilla JavaScript.

**Theme:** Invaders are split in thirds — Austria (red/white/red), Belgium (black/yellow/red), and US (stars & stripes); player shots are pixel-art French fries and enemy shots are ketchup blobs.

**Build tag:** `flag-edition-v15-nifi-listenhttp` (shown on the start screen).

## How to Run

**Option 1 — Open directly**

Double-click `index.html` or open it in any modern browser. Hard-refresh (`Cmd+Shift+R` / `Ctrl+Shift+R`) if assets look stale.

**Option 2 — Local static server (recommended for NiFi / CORS testing)**

From the repository root:

```bash
python3 -m http.server 8080
```

Then open [http://localhost:8080/index.html](http://localhost:8080/index.html).

> **404?** Start the server in the folder that contains `index.html`.

Mac setup notes: [sync-local-mac.md](./sync-local-mac.md)

## How to Play

### Desktop (keyboard)

| Control | Action |
|---------|--------|
| `←` `→` or `A` `D` | Move ship left / right |
| `Space` (or `↑` / `W`) | Fire |
| `Space` / `Enter` | Start game / Restart after game over |

### Mobile / iPhone (touch)

| Control | Action |
|---------|--------|
| **◀** / **▶** buttons | Move ship left / right |
| **FIRE** button | Shoot |
| Tap game screen | Start / Restart |
| Tap **PLAYER** name on intro | Change pilot name |

On iPhone Safari the game fills the viewport (portrait or landscape), respects safe areas, and uses on-screen controls.

### Rules

- Destroy all invaders to advance to the next level.
- Invaders speed up as their numbers drop.
- Green bunkers absorb shots; they erode pixel by pixel.
- **10 lives** — lose them all and it's game over.
- Score: top row 30 pts, middle 20 pts, bottom 10 pts.

## Player name

On first visit the game prompts for a pilot name (**PLAYER: {NAME}** on the intro — click to change). Events include `playerId` and `playerName`.

## Event streaming (console mock)

Enable structured JSON events in DevTools:

```text
index.html?events=1
```

Or persist: `localStorage.setItem('si-events', '1')`.

Schema reference: [kafka-streaming-proposal.md](./kafka-streaming-proposal.md) (event shapes only; default transport is **NiFi ListenHTTP**, not Kafka).

## NiFi ListenHTTP export (primary)

When NiFi mode is on, the game **POST**s JSON to your Apache NiFi 2 **ListenHTTP** URL (`Content-Type: application/json`).

| Doc | Purpose |
|-----|---------|
| [enable-nifi-events.md](./enable-nifi-events.md) | Turn on `?nifi=1` and projtriton URL |
| [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md) | ListenHTTP, CORS, curl |
| [nifi-browser-cors.md](./nifi-browser-cors.md) | Browser POST vs proxy |
| [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md) | NiFi → Iceberg (CDP) |
| [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl) | Trino DDL |
| [hive-iceberg-space-invaders.ddl](./hive-iceberg-space-invaders.ddl) | Hive DDL samples |

**localStorage keys:** `si-nifi`, `si-nifi-listen-url`, `si-events`, `si-player-name`

Example local URL:

```text
http://127.0.0.1:9998/space-invaders
```

Example curl:

```bash
./curl-smoke-test.sh
```

Simulate many players: `./simulate-players-nifi.sh`

## Top 10 leaderboard (optional)

Flink SSB-backed ranking UI: [top10-leaderboard.html](./top10-leaderboard.html) — see [top10-leaderboard.md](./top10-leaderboard.md). Requires `SSB_USER` / `SSB_PASSWORD` when using [serve-with-nifi-proxy.py](./serve-with-nifi-proxy.py).

## Dev tools in this repo

| File | Purpose |
|------|---------|
| `serve-with-nifi-proxy.py` | Same-origin proxy for NiFi POST and SSB GET |
| `simulate-players-nifi.py` / `.sh` | Synthetic game events to NiFi |
| `curl-smoke-test.sh` | One-shot ingest smoke test |

## License

MIT — see [LICENSE](./LICENSE).

## Not included in GitHub

The optional ~2.7 MB `kafka-browser-producer.js` bundle and `scripts/node_modules` are omitted to keep the repo small. See [public-kafka-ingest.md](./public-kafka-ingest.md) for historical Kafka notes.
