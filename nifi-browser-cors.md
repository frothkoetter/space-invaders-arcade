# Browser CORS and NiFi ListenHTTP

The game POSTs JSON to your NiFi **ListenHTTP** URL. From `http://127.0.0.1:8080` to a Cloudera **projtriton** inbound host, the browser treats that as **cross-origin**.

With `Content-Type: application/json`, the browser sends an **OPTIONS preflight**. If ListenHTTP (or the gateway in front of it) does not return CORS headers, the POST never runs and DevTools shows a CORS error.

The game body is unchanged: flat snake_case fields and `data_json` (same as Avro/Hive DDL). Only the HTTP transport mode differs.

## (a) Simple request mode (default)

**No preflight:** POST uses `Content-Type: text/plain;charset=UTF-8` with the same JSON string in the body. Cross-origin posts use `fetch` with `mode: no-cors` so the browser still delivers the request even when the response is opaque.

Open (direct to projtriton):

```text
http://127.0.0.1:8080/index.html?nifi=1&events=1&nifi-url=http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

Optional explicit mode (stored in `localStorage` as `si-nifi-mode`):

```text
&nifi-mode=simple
```

Console on load should show:

```text
[nifi] mode=simple (Content-Type: text/plain;charset=UTF-8, JSON body; skips OPTIONS preflight) → …
```

On tab close, queued events may fall back to **`navigator.sendBeacon`** with the same text/plain JSON body.

**Legacy JSON + preflight** (use after NiFi/gateway adds CORS):

```text
&nifi-mode=cors
```

## (b) Local dev proxy (same-origin ingest)

Run from the `space-invaders` folder (Mac/Linux):

```bash
cd docs/space-invaders   # or your synced copy of Context → docs/space-invaders
python3 serve-with-nifi-proxy.py
```

Override upstream:

```bash
NIFI_UPSTREAM='http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders' python3 serve-with-nifi-proxy.py
```

Then open:

```text
http://127.0.0.1:8080/index.html?nifi=1&events=1&nifi-url=http://127.0.0.1:8080/space-invaders
```

The proxy serves static files and forwards `POST /space-invaders` (and `OPTIONS` with `Access-Control-Allow-Origin: *`) to NiFi. You can use `nifi-mode=cors` here if you want readable HTTP status in the console.

## (c) Production: NiFi / gateway CORS

For browsers to POST with `application/json` and read responses cross-origin, the **ingress in front of ListenHTTP** (or NiFi itself, if exposed directly) should respond to OPTIONS and POST with at least:

- `Access-Control-Allow-Origin: https://your-game-origin` (or `*` for dev-only)
- `Access-Control-Allow-Methods: POST, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type`

Until that exists, prefer **simple mode** (default) or the **local proxy**. Server-side **`curl`** is not subject to CORS — see [curl-smoke-test.sh](./curl-smoke-test.sh) and [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md).

## Related

- [enable-nifi-events.md](./enable-nifi-events.md) — query params and localStorage keys
- [index.html](./index.html) — `postNifiEvents`, `getNifiListenUrl`, `getNifiPostMode`
