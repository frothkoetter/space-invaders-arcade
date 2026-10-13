# Top 10 pilots leaderboard (Flink SSB)

Live ranking UI: [top10-leaderboard.html](./top10-leaderboard.html)

Data source: Flink SSB materialized view `top10player` on the trition-si gateway (1-minute windows, `player_rank`, `player_name`, `score`).

## Run locally (recommended)

The browser cannot call the gateway with CDP Basic auth directly (CORS). Use the dev server proxy and **environment variables** for credentials — do not commit passwords.

```bash
cd /path/to/space-invaders-arcade   # git clone root — see sync-local-mac.md

export SSB_USER='your-cdp-user'
export SSB_PASSWORD='your-cdp-password'
python3 serve-with-nifi-proxy.py
```

See [sync-local-mac.md](./sync-local-mac.md) for clone and Mac setup.

Open (any of these):

```text
http://127.0.0.1:8080/top10-leaderboard.html
http://127.0.0.1:8080/leaderboard
```

If you get **404**, confirm `top10-leaderboard.html` is in the repo root next to `serve-with-nifi-proxy.py`, then restart the server.

The page calls same-origin `GET /api/ssb/top10player?limit=100` (proxied with Basic auth).

## Direct mode (no proxy)

Open the HTML with `?proxy=0` and use **CDP LOGIN** — username/password stored in `sessionStorage` only. Works only if the gateway sends CORS headers allowing your origin (often it does not).

## Query params

| Param | Default | Meaning |
|-------|---------|---------|
| `limit` | `100` | Passed to SSB API |
| `proxy` | `1` | `0` = direct gateway URL |
| `api` | built-in gateway URL | Override MVE endpoint |

Auto-refresh every 20 seconds; dropdown selects historical 1-minute windows.
