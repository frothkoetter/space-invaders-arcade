# Run on your Mac (clone or copy)

This repository is the **standalone** Space Invaders game and docs. You do not need Cursor Agent Store sync to play locally.

## Quick start

```bash
git clone git@github.com:YOUR_USER/space-invaders-arcade.git
cd space-invaders-arcade
grep "flag-edition-v15-nifi-listenhttp" index.html   # sanity check
python3 -m http.server 8080
```

Open [http://localhost:8080/index.html](http://localhost:8080/index.html).

Hard refresh after updates: **Cmd+Shift+R**.

## Game + NiFi ingest (local)

**Terminal A — NiFi ListenHTTP** on port `9998`, path `/space-invaders` (see [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md)).

**Terminal B — static server** (repo root):

```bash
python3 -m http.server 8080
```

Browser:

```text
http://localhost:8080/index.html?nifi=1&nifi-url=http://127.0.0.1:9998/space-invaders&events=1
```

Same-origin proxy for CORS: [serve-with-nifi-proxy.py](./serve-with-nifi-proxy.py) · [nifi-browser-cors.md](./nifi-browser-cors.md)

## Top 10 leaderboard (Flink SSB — optional)

Requires CDP credentials via **environment variables** (never commit passwords):

```bash
export SSB_USER='your-cdp-user'
export SSB_PASSWORD='your-cdp-password'
python3 serve-with-nifi-proxy.py
```

Open [http://127.0.0.1:8080/top10-leaderboard.html](http://127.0.0.1:8080/top10-leaderboard.html). Details: [top10-leaderboard.md](./top10-leaderboard.md).

## Simulate traffic

```bash
./simulate-players-nifi.sh
# or: python3 simulate-players-nifi.py
```

Set `NIFI_URL` if NiFi is not on the default local URL.

## Still using Cursor Project Context?

The ArcadeSpaceinvader project may carry newer edits before they land in GitHub. After pulling from GitHub, compare `index.html` build tag with Context if behavior differs.
