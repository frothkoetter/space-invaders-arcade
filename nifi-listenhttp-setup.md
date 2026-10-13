# Apache NiFi 2 — ListenHTTP ingest for Space Invaders events

The game (**flag-edition-v15-nifi-listenhttp**) sends structured game events with **HTTP POST** + `Content-Type: application/json` to a NiFi **ListenHTTP** processor. This is **not** raw TCP and not a separate forwarder service.

**Example ingest URL (local dev):** `http://nifi-host:9998/space-invaders`

**Cloudera inbound (projtriton):** `http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders` — port **9088**, path **`/space-invaders`**, **plain HTTP** (not TLS on 9088; `curl https://…:9088` fails with SSL protocol/version errors).

**Iceberg landing zone (NiFi 2 → Trino):** [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md) · DDL: [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl)

---

## 1. Add ListenHTTP to a flow

1. Open the NiFi 2 UI and create or open a process group (e.g. `space-invaders-ingest`).
2. Drag **ListenHTTP** onto the canvas.
3. Configure at minimum:

| Property | Example | Notes |
|----------|---------|--------|
| **Listening Port** | `9998` | Must be reachable from the browser (or from your reverse proxy). |
| **Base Path** | `/space-invaders` | Full URL path segment; game POSTs to `http://<host>:9998/space-invaders`. |
| **HTTP Methods Allowed** | `POST` | Restrict to POST only for ingest. |
| **Allowed Paths** | (default / empty) | Use Base Path as documented in your NiFi version. |
| **Max Data to Receive per Second** | tune as needed | Prevent runaway clients. |
| **Return Code** | `200` | Browser fire-and-forget expects success; failures only show in DevTools. |

4. **Start** the ListenHTTP processor (and the parent group if required).

Each POST body becomes one FlowFile. The game sends either:

- a **single event object** (envelope JSON), or  
- a **JSON array** of envelopes when events are batched (up to 50 per request).

Downstream you typically **split** arrays (see sketch below) or use **RecordReader** if you normalize to JSON Lines.

---

## 2. Downstream flow (optional sketch)

Minimal debug path:

```text
ListenHTTP → LogAttribute → (terminate or loop)
```

Production-style path:

```text
ListenHTTP
  → EvaluateJsonPath (optional: detect array vs object)
  → SplitJson (if root is array; JsonPath `$.*`)
  → UpdateAttribute (e.g. mime.type = application/json)
  → PublishKafka / PutKafka (topic e.g. space-invaders-events)
  → (optional) PutS3Object / Elasticsearch / etc.
```

**LogAttribute** — set **Log Level** to `info`, log `flowfile.uuid`, `${filename}`, and `${http.context.path}` if exposed as attributes, to verify POSTs arrive.

**PutKafka** — set **Topic Name**, **Message Key** (e.g. `${eventType}` via RecordPath if you parse JSON), and broker properties for your cluster.

Auto-terminate **failure** and **not found** relationships on ListenHTTP only after you have error handling elsewhere.

---

## 3. SSL/TLS

For **HTTPS** ListenHTTP:

1. Configure NiFi’s **SSL Context Service** (keystore/truststore) on the controller or processor as required by your NiFi 2 docs.
2. Set ListenHTTP to use that context and listen on **443** or **8443** (or terminate TLS at a load balancer).
3. In the game, set the full URL in localStorage:

```javascript
localStorage.setItem('si-nifi-listen-url', 'https://nifi.example.com/space-invaders')
```

Browsers require a **trusted certificate** (public CA or your machine trust store). Self-signed certs fail unless the user accepts them by opening the NiFi URL in a tab first (not ideal for production).

---

## 4. Authentication

ListenHTTP itself does not replace NiFi UI login. Common patterns:

| Pattern | Browser POST |
|---------|----------------|
| **No auth on ingest path** | Simplest for local dev; restrict by network/firewall. |
| **Reverse proxy (Basic / Bearer / mTLS)** | Proxy validates auth; forwards to ListenHTTP on localhost. Game cannot send custom headers without code changes — prefer network-level or same-origin proxy. |
| **NiFi restricted / internal only** | Game served from same VPN; ListenHTTP bound to internal IP. |

For local Mac dev, binding ListenHTTP to `0.0.0.0:9998` and POSTing from `http://localhost:8080` is usually enough without auth.

---

## 5. CORS (browser → NiFi)

The game runs in the browser (e.g. `http://localhost:8080`). POSTs to another origin (e.g. `http://nifi-host:9998`) are **cross-origin**. The browser may block the response (or preflight) unless NiFi or a front proxy sends **CORS** headers.

**ListenHTTP** in stock NiFi often does **not** add `Access-Control-Allow-Origin`. Symptoms:

- DevTools → Network: POST shows **CORS error** or failed preflight (`OPTIONS`).
- Game still plays; console may show `[nifi] POST failed`.

**Options:**

1. **Same-origin reverse proxy** (recommended for production)  
   Serve static game and proxy `/space-invaders` to NiFi on the **same host:port** as the game, e.g. nginx:

   ```nginx
   location /space-invaders {
     proxy_pass http://127.0.0.1:9998/space-invaders;
     add_header Access-Control-Allow-Origin $http_origin always;
     add_header Access-Control-Allow-Methods POST, OPTIONS always;
     add_header Access-Control-Allow-Headers Content-Type always;
     if ($request_method = OPTIONS) { return 204; }
   }
   ```

   Then set `si-nifi-listen-url` to `http://localhost:8080/space-invaders` (same origin — no CORS).

2. **Add CORS at NiFi** (if your version supports custom response headers on ListenHTTP or via a follow-on processor — often easier via proxy).

3. **Host game static files from NiFi** (same origin as ListenHTTP) — uncommon but valid for demos.

4. **Non-browser clients** — `curl` is not subject to CORS; use for pipeline testing without fixing CORS first.

The game uses **simple POST** with `Content-Type: application/json` (may trigger preflight). Fixing CORS on the **ingest URL** is required for reliable browser export.

---

## 6. Verify with curl (matches browser body)

Single event (one object):

```bash
curl -sS -X POST 'http://nifi-host:9998/space-invaders' \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "00000000-0000-4000-8000-000000000001",
    "eventType": "invader.killed",
    "occurredAt": "2026-10-12T12:00:00.000Z",
    "sessionId": "test-session",
    "playerId": "anon-test",
    "playerName": "PILOT",
    "gameVersion": "flag-edition-v15-nifi-listenhttp",
    "level": 1,
    "frame": 100,
    "data": { "pointsAwarded": 10 }
  }'
```

Batched array (as the game sends when flushing multiple events):

```bash
curl -sS -X POST 'http://nifi-host:9998/space-invaders' \
  -H 'Content-Type: application/json' \
  -d '[{"eventType":"score.changed","eventId":"a"},{"eventType":"invader.killed","eventId":"b"}]'
```

Check NiFi **Bulletin Board** and **LogAttribute** output for new FlowFiles.

---

## 7. Game configuration (quick reference)

Enable NiFi export in the browser:

```javascript
localStorage.setItem('si-nifi', '1')
localStorage.setItem('si-nifi-listen-url', 'http://nifi-host:9998/space-invaders')
localStorage.setItem('si-events', '1')   // optional console [game-event] logs
```

Or one-shot URL:

```text
http://localhost:8080/index.html?nifi=1&nifi-url=http://nifi-host:9998/space-invaders&events=1
```

See [README.md](./README.md) for full localStorage keys and Mac static-server steps.

---

## 8. NiFi flow summary

| Step | Component | Role |
|------|-----------|------|
| 1 | **ListenHTTP** | Accept POST JSON on `/space-invaders`, port e.g. **9998** |
| 2 | **SplitJson** (optional) | One FlowFile per event when body is an array |
| 3 | **LogAttribute** | Debug attributes and payload size |
| 4 | **PutKafka** / **PublishKafka** | Stream to Kafka for analytics |
| 5 | Proxy + CORS (if needed) | Same-origin or CORS headers for browser on `localhost:8080` |

Event schema: [kafka-streaming-proposal.md](./kafka-streaming-proposal.md) (transport is now **NiFi ListenHTTP**, not Kafka-from-browser).

For **ListenHTTP → Iceberg → Trino**, use [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md) (Cloudera host **9088**).
