# Run on iPhone (standalone / Add to Home Screen)

The game is built for **iPhone Safari**: touch controls, `viewport-fit=cover`, safe-area padding, and **full-screen** when launched from the home screen (`apple-mobile-web-app-capable`).

You need a **real HTTP(S) URL** on the phone. **`file://` and “Files app → open HTML” do not work** for a reliable standalone install.

---

## Option 1 — Hosted game (recommended)

Use any public origin where this repo is served (GitHub Pages, CDN, etc.).

1. On the iPhone, open **Safari**.
2. Go to the game URL, for example:

   ```text
   https://frothkoetter.github.io/space-invaders-arcade/index.html
   ```

   (Enable GitHub Pages on branch `main` / root if that host is not live yet.)

3. **NiFi export on first visit** — open the full bookmark URL once so settings save to `localStorage` (see [enable-nifi-events.md](./enable-nifi-events.md) Option A), then continue with step 4.

4. Tap **Share** (square with arrow) → **Add to Home Screen**.
5. Edit the name if you like (e.g. `Space Invaders`) → **Add**.
6. Launch from the new icon. Safari chrome is hidden; use **◀** **FIRE** **▶** and **tap the canvas** to start.

After the first configured visit, the home-screen icon can point at the short URL (`…/index.html` only); NiFi/player settings stay until you clear Safari data for that site.

---

## Option 2 — Same Wi‑Fi as your Mac (local dev)

Serve the repo on your Mac and use your **LAN IP** (not `127.0.0.1` — that is the phone itself).

**On the Mac** (repo root):

```bash
python3 -m http.server 8080 --bind 0.0.0.0
```

Find the Mac’s IP (System Settings → Network, or `ipconfig getifaddr en0`).

**On the iPhone** (Safari):

```text
http://192.168.1.42:8080/index.html
```

Replace `192.168.1.42` with your Mac’s address. Mac firewall must allow incoming connections on port **8080**.

Then **Share → Add to Home Screen** as in Option 1.

For NiFi from the phone, use a ListenHTTP URL reachable from the phone (projtriton inbound or your LAN NiFi), e.g.:

```text
http://192.168.1.42:8080/index.html?nifi=1&events=1&nifi-mode=simple&nifi-url=http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

---

## Controls (standalone)

| Action | Gesture |
|--------|---------|
| Move | **◀** / **▶** (hold for continuous move) |
| Fire | **FIRE** |
| Start / restart | Tap the game canvas |
| Pilot name | Tap **PLAYER: …** on the intro screen |

Works in **portrait** and **landscape** (landscape hides the title bar and hint text to save space).

---

## Tips

- **Hard refresh** after updates: close the app from the app switcher, reopen; or clear website data for that origin in Settings → Safari.
- **Audio** starts after the first tap (iOS autoplay policy).
- **Leaderboard** ([top10-leaderboard.html](./top10-leaderboard.html)) can be added to the home screen the same way; it still needs a reachable server and (usually) the [SSB proxy](./top10-leaderboard.md) for live data.

---

## Related

- [README.md](./README.md) — mobile controls summary  
- [enable-nifi-events.md](./enable-nifi-events.md) — NiFi query params  
- [nifi-browser-cors.md](./nifi-browser-cors.md) — cross-origin POST from mobile Safari  
