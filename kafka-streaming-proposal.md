# Space Invaders — Kafka Event Streaming Proposal

> **Implementation note:** Phase 1 console mock remains. **Phase 2 (v13):** the browser POSTs to **`tcp-forward/`** at `http://localhost:3002/send`; the forwarder writes JSON lines to a raw TCP host/port (like piping to `nc`). Kafka / `event-proxy/` were removed. See [README.md](./README.md#tcp-event-export-phase-2--browser--http-forwarder--raw-tcp).

**Game:** `flag-edition-v4-mobile` (single-file canvas game)  
**Source:** `docs/space-invaders/index.html`  
**Status:** Proposal only — no implementation yet

---

## Executive summary

This document proposes adding a non-blocking event stream from the browser game to a Kafka topic (`arcade.space-invaders.events`) via a small backend proxy. The browser never talks to Kafka directly; it emits structured JSON events at well-defined hooks in the existing game loop. A proxy validates, enriches, and produces records.

Recommended path for a static HTML game: **browser → REST proxy → Kafka** (Option A), with an optional offline retry buffer in the client.

---

## 1. Event catalog

All events share a common envelope. Per-event payloads live in `data`.

### 1.1 Common envelope schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["eventId", "eventType", "occurredAt", "sessionId", "gameVersion", "data"],
  "properties": {
    "eventId": { "type": "string", "format": "uuid", "description": "Client-generated idempotency key" },
    "eventType": { "type": "string", "enum": [
      "invader.killed",
      "score.changed",
      "player.destroyed",
      "lives.changed",
      "level.completed",
      "bunker.friendly_fire"
    ]},
    "occurredAt": { "type": "string", "format": "date-time", "description": "ISO-8601 UTC" },
    "sessionId": { "type": "string", "description": "UUID per browser tab / play session" },
    "playerId": { "type": "string", "description": "Optional anonymous player id (localStorage)" },
    "gameVersion": { "type": "string", "description": "Build tag, e.g. flag-edition-v4-mobile" },
    "level": { "type": "integer", "minimum": 1 },
    "frame": { "type": "integer", "minimum": 0, "description": "Game frame counter at emit time" },
    "data": { "type": "object" }
  }
}
```

### 1.2 Event types and payloads

| eventType | When (game logic) | `data` schema |
|-----------|-------------------|---------------|
| `invader.killed` | Player bullet destroys invader | See below |
| `score.changed` | Score increments after kill | See below |
| `player.destroyed` | Invader bullet hits player ship | See below |
| `lives.changed` | Lives decrement (or reset on new game) | See below |
| `level.completed` | All invaders cleared; `nextLevel()` runs | See below |
| `bunker.friendly_fire` | Player bullet erodes bunker pixels | See below |

#### `invader.killed`

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "invader.killed",
  "occurredAt": "2026-10-08T16:20:01.234Z",
  "sessionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "playerId": "anon-8f3a2b1c",
  "gameVersion": "flag-edition-v4-mobile",
  "level": 2,
  "frame": 4821,
  "data": {
    "invaderRow": 0,
    "invaderCol": 5,
    "flag": "belgium",
    "pointsAwarded": 30,
    "position": { "x": 320, "y": 70 },
    "remainingInvaders": 54
  }
}
```

#### `score.changed`

```json
{
  "eventId": "8d0e7780-8536-51ef-a55c-f18gd2g01bf8",
  "eventType": "score.changed",
  "occurredAt": "2026-10-08T16:20:01.235Z",
  "sessionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "gameVersion": "flag-edition-v4-mobile",
  "level": 2,
  "frame": 4821,
  "data": {
    "previousScore": 120,
    "newScore": 150,
    "delta": 30,
    "reason": "invader_kill"
  }
}
```

#### `player.destroyed`

```json
{
  "eventId": "9e1f8891-9647-62fg-b66d-g29he3h12cg9",
  "eventType": "player.destroyed",
  "occurredAt": "2026-10-08T16:20:15.100Z",
  "sessionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "gameVersion": "flag-edition-v4-mobile",
  "level": 2,
  "frame": 5102,
  "data": {
    "position": { "x": 400, "y": 550 },
    "cause": "invader_bullet",
    "livesRemainingAfter": 9,
    "gameOver": false
  }
}
```

#### `lives.changed`

```json
{
  "eventId": "af20aa02-a758-73gh-c77e-h3aif4i23dh0",
  "eventType": "lives.changed",
  "occurredAt": "2026-10-08T16:20:15.101Z",
  "sessionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "gameVersion": "flag-edition-v4-mobile",
  "level": 2,
  "frame": 5102,
  "data": {
    "previousLives": 10,
    "newLives": 9,
    "reason": "player_hit"
  }
}
```

#### `level.completed`

```json
{
  "eventId": "bg31bb13-b869-84hi-d88f-i4bjg5j34ei1",
  "eventType": "level.completed",
  "occurredAt": "2026-10-08T16:21:00.000Z",
  "sessionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "gameVersion": "flag-edition-v4-mobile",
  "level": 2,
  "frame": 6200,
  "data": {
    "completedLevel": 2,
    "nextLevel": 3,
    "scoreAtCompletion": 450,
    "livesRemaining": 8,
    "durationMs": 142000
  }
}
```

#### `bunker.friendly_fire`

```json
{
  "eventId": "ch42cc24-c97a-95ij-e99g-j5ckh6k45fj2",
  "eventType": "bunker.friendly_fire",
  "occurredAt": "2026-10-08T16:20:05.500Z",
  "sessionId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "gameVersion": "flag-edition-v4-mobile",
  "level": 1,
  "frame": 3200,
  "data": {
    "bunkerIndex": 1,
    "bunkerPosition": { "x": 240, "y": 470 },
    "pixelHit": { "row": 4, "col": 7 },
    "pixelsDestroyed": 9,
    "bulletPosition": { "x": 245, "y": 468 }
  }
}
```

### 1.3 Optional session lifecycle events (future)

Not in scope for MVP, but useful later: `session.started`, `session.game_over`, `session.restarted`. These would hook `startGame()` / `restartGame()` and the `STATE.GAMEOVER` transitions.

---

## 2. When each event fires — code hook map

The game uses a single `requestAnimationFrame` loop (`gameLoop` → `update` → `updateBullets` / `updateInvaders`). All gameplay events originate in collision and state-transition code.

| Event | Function | Line region (approx.) | Trigger condition |
|-------|----------|----------------------|-------------------|
| `bunker.friendly_fire` | `updateBullets()` → `bulletHitsBunker()` | 749–751, 650–672 | Player bullet (`fromPlayer: true`) hits bunker pixel grid |
| `invader.killed` | `updateBullets()` | 753–759 | `rectsOverlap(b, inv)` and `inv.alive` becomes `false` |
| `score.changed` | `updateBullets()` | 757 | Immediately after `score += inv.type.points` |
| `player.destroyed` | `updateBullets()` | 773–779 | Invader bullet overlaps player |
| `lives.changed` | `updateBullets()` | 774 | On `lives--`; also emit from `startGame()` when lives reset to `START_LIVES` (10) |
| `level.completed` | `updateInvaders()` → `nextLevel()` | 689–692, 390–399 | `getAliveInvaders().length === 0` before `nextLevel()` increments level |

### 2.1 Recommended hook placement

**Centralize emits** in a single helper rather than scattering fetch calls:

```
publishGameEvent(type, data)  // fire-and-forget, never throws into game loop
```

Call sites:

1. **`bulletHitsBunker(bullet, bunker)`** — After pixel destruction (line ~668), only if `bullet.fromPlayer === true`. Return bunker index and hit coordinates (extend return value or pass index into function).

2. **`updateBullets()` — invader kill block** (lines 755–759):
   - Emit `invader.killed` with inv metadata (`row`, `col`, `flag`, `type.points`, position).
   - Emit `score.changed` with `previousScore`, `newScore`, `delta`.

3. **`updateBullets()` — player hit block** (lines 773–779):
   - Emit `player.destroyed` with `cause: "invader_bullet"`, `gameOver: lives <= 0` after decrement.
   - Emit `lives.changed` with before/after values.

4. **`updateInvaders()` — level clear** (lines 689–692):
   - Before calling `nextLevel()`, emit `level.completed` with `completedLevel: level`, `nextLevel: level + 1`, current score and lives.

5. **`startGame()`** (lines 372–384):
   - Generate or reuse `sessionId`.
   - Optionally emit `lives.changed` with `reason: "game_start"` and `newLives: START_LIVES`.

### 2.2 Events not currently distinct in code

- **Invader reaches player (game over):** `updateInvaders()` sets `state = STATE.GAMEOVER` at line 717 without decrementing lives. Consider a future `game.over` event with `cause: "invaders_landed"`.
- **`STATE.WIN`:** Defined but never set in current code; no win event until win logic exists.

### 2.3 Ordering guarantees

Within a single frame, emit order should be:

1. `invader.killed` → `score.changed` (same kill)
2. `player.destroyed` → `lives.changed` (same hit)
3. `level.completed` (once per wave clear)

The proxy can rely on `occurredAt` + `frame` for ordering; Kafka partition key (`sessionId`) preserves per-session order.

---

## 3. Architecture options

### Option A: Browser → REST proxy → Kafka (recommended)

```
[Static HTML] --POST /events--> [Node/Python proxy] --produce--> [Kafka: arcade.space-invaders.events]
```

| Pros | Cons |
|------|------|
| Simplest for static hosting (GitHub Pages, S3, CDN) | One HTTP round-trip per event (mitigate with batching) |
| Standard CORS + API key auth at proxy | Slightly higher latency than WebSocket |
| Easy local dev with mock sink | |
| Works through corporate firewalls | |
| Proxy hides Kafka credentials | |

**Best fit:** This project is a single HTML file with no build step. REST + fire-and-forget `fetch` (or `navigator.sendBeacon` on unload) is the lowest-friction integration.

### Option B: Browser → WebSocket gateway → Kafka

```
[Static HTML] <--WS--> [Gateway service] --produce--> [Kafka]
```

| Pros | Cons |
|------|------|
| Persistent connection; lower per-event overhead | More infra (WS server, reconnect logic, sticky sessions) |
| Natural bidirectional channel (future: live leaderboard push) | Harder to scale publicly; connection limits |
| Can batch events in memory before produce | Overkill for ~1–5 events/sec per player |

**Best fit:** If downstream needs sub-second fan-out to many viewers (spectator mode, live dashboard). Not needed for MVP analytics.

### Option C: Kafka REST Proxy / Confluent HTTP API

```
[Static HTML] --POST--> [Kafka REST Proxy / Confluent Cloud REST] --> [Kafka]
```

| Pros | Cons |
|------|------|
| No custom proxy code if schema registry + REST proxy already deployed | **Never expose cluster credentials in browser** — even with API keys, REST proxy is a high-value attack surface |
| Confluent Cloud REST v3 supports JSON produce | CORS often disabled by default; must configure |
| | Schema validation at edge is weaker without a dedicated validation layer |
| | Rate limiting and abuse prevention still required in front |

**Best fit:** Teams already running Confluent REST Proxy with an API gateway in front. Still recommend a thin app-specific proxy for validation and scoped tokens rather than exposing REST Proxy directly to anonymous browsers.

### Recommendation

**Option A** for MVP and production. Add WebSocket (Option B) only if live fan-out becomes a requirement. Use Confluent REST (Option C) behind the app proxy, not from the browser.

---

## 4. Public endpoint considerations

### 4.1 Authentication

| Approach | Use case |
|----------|----------|
| **Scoped publish token (recommended)** | Short-lived or rotatable token issued by proxy `/auth/session` on game load; limited to `POST /events` on one topic |
| **API key in header** | Acceptable for demos; rotate frequently; restrict by origin + rate limit |
| **No auth** | Dev/mock only — will be scraped and spammed on a public arcade |

The browser receives only a **publish-scoped token**, never Kafka SASL credentials or admin keys.

### 4.2 Rate limiting

- **Per sessionId:** ~30 events/minute (typical peak: rapid firing + kills ≈ 5–10/min)
- **Per IP:** 100 requests/minute
- **Global:** circuit breaker on proxy if Kafka lag exceeds threshold
- Return `429` with `Retry-After`; client buffer retries with exponential backoff

### 4.3 Partitioning key

```
key = sessionId
```

Rationale: All events for one play session land in one partition, preserving order for replay and session analytics. Optional secondary topic keyed by `playerId` for cross-session player history (future).

### 4.4 Idempotency

- Client generates `eventId` (UUID v4) per event.
- Proxy checks Redis/in-memory dedup cache (TTL 24h) or Kafka idempotent producer + compacted idempotency topic.
- Retries from client offline buffer reuse the same `eventId`.

### 4.5 PII and data minimization

| Field | PII risk | Guidance |
|-------|----------|----------|
| `sessionId` | Low | Random UUID per tab session |
| `playerId` | Low if anonymous | Generate `anon-<uuid>` in localStorage; no email/username |
| `position` | None | Canvas coordinates only |
| IP address | Medium | Strip or hash at proxy before downstream; do not store in event payload |

Do not collect: geolocation, device fingerprints, free-text input (game has none).

---

## 5. Client integration sketch

Add ~80–120 lines to `index.html` in a dedicated `// --- Event streaming ---` block. No changes to render path.

### 5.1 Configuration

```javascript
const EVENT_STREAM = {
  enabled: true,
  endpoint: 'https://arcade.example.com/api/v1/events',
  gameVersion: 'flag-edition-v4-mobile',
  batchMax: 10,
  flushIntervalMs: 2000,
  mock: false  // true → console.log only
};
```

Load publish token from meta tag or `GET /auth/session` once at `startGame()`.

### 5.2 Core helper

```javascript
let sessionId = crypto.randomUUID();
let eventQueue = [];
let flushTimer = null;

function publishGameEvent(eventType, data) {
  const event = {
    eventId: crypto.randomUUID(),
    eventType,
    occurredAt: new Date().toISOString(),
    sessionId,
    playerId: getOrCreatePlayerId(),
    gameVersion: EVENT_STREAM.gameVersion,
    level,
    frame,
    data
  };

  if (EVENT_STREAM.mock || !EVENT_STREAM.enabled) {
    console.debug('[game-event]', event);
    return;
  }

  eventQueue.push(event);
  scheduleFlush();
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(flushEvents, EVENT_STREAM.flushIntervalMs);
  if (eventQueue.length >= EVENT_STREAM.batchMax) flushEvents();
}

async function flushEvents() {
  flushTimer = null;
  if (eventQueue.length === 0) return;
  const batch = eventQueue.splice(0, EVENT_STREAM.batchMax);
  try {
    await fetch(EVENT_STREAM.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${publishToken}`
      },
      body: JSON.stringify({ events: batch }),
      keepalive: true
    });
  } catch (err) {
    eventQueue.unshift(...batch);  // re-queue
    persistOfflineBuffer(batch);
  }
}
```

### 5.3 Design constraints

- **Non-blocking:** Never `await` inside `update()` / collision handlers; only queue.
- **No game impact:** Wrap helper in try/catch; failures are silent to the player.
- **Offline buffer:** `localStorage` key `si-event-buffer` (cap 200 events, FIFO drop oldest).
- **Page unload:** `navigator.sendBeacon` for remaining queue.
- **Batching:** Reduces HTTP overhead during invader kill streaks.

### 5.4 Example call site (invader kill)

```javascript
if (rectsOverlap(b, inv)) {
  const prevScore = score;
  inv.alive = false;
  score += inv.type.points;
  publishGameEvent('invader.killed', { /* ... */ });
  publishGameEvent('score.changed', {
    previousScore: prevScore,
    newScore: score,
    delta: inv.type.points,
    reason: 'invader_kill'
  });
  beep(200, 0.08, 'square', 0.06);
  return false;
}
```

---

## 6. Backend sketch

Minimal Node.js proxy (Python/FastAPI equivalent is fine).

### 6.1 Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness |
| `POST` | `/auth/session` | Return scoped publish token + `sessionId` hint |
| `POST` | `/api/v1/events` | Accept `{ events: [...] }` batch, validate, produce |

### 6.2 Processing pipeline

1. Verify Bearer token (JWT or opaque token lookup).
2. Validate each event against JSON Schema (Ajv / jsonschema).
3. Reject unknown `eventType` or oversize payloads (> 4 KB).
4. Enrich with `receivedAt`, `sourceIpHash` (server-side only).
5. Produce to Kafka with key = `sessionId`.

### 6.3 Kafka topic design

```
Topic:  arcade.space-invaders.events
Partitions: 6–12 (scale with concurrent sessions)
Retention: 7 days (analytics); 30 days if compliance allows
Cleanup: delete
Compression: lz4 or zstd
```

Optional compacted topic for dedup:

```
Topic:  arcade.space-invaders.event-idempotency
Key:    eventId
Value:  receivedAt
```

### 6.4 Example produce (kafkajs)

```javascript
await producer.send({
  topic: 'arcade.space-invaders.events',
  messages: events.map(e => ({
    key: e.sessionId,
    value: JSON.stringify({ ...e, receivedAt: new Date().toISOString() })
  }))
});
```

### 6.5 Deployment

- Container: single replica for MVP; HPA on CPU/request rate for production.
- Secrets: `KAFKA_BROKERS`, `KAFKA_SASL_*` via env / secret manager — never in repo.
- CORS: allow only known game origins (`https://arcade.example.com`).

---

## 7. Security warnings

1. **Never embed Kafka credentials in the browser.** SASL username/password, API secrets, and Schema Registry credentials stay server-side only.

2. **Never expose Kafka REST Proxy directly** to anonymous internet without an auth and validation layer.

3. **Use scoped, rotatable publish tokens** with minimal scope (produce-only, single topic, short TTL).

4. **Validate all payloads server-side.** Client-side validation is UX only; attackers can POST arbitrary JSON.

5. **Rate limit aggressively** on public endpoints; expect bots and pentesters.

6. **Do not log full IP addresses** in Kafka payloads; hash at proxy if needed for abuse detection.

7. **CORS is not security.** It only protects browsers; server-to-server abuse requires token + rate limits.

8. **Sanitize error responses.** Do not leak Kafka broker hostnames or internal stack traces to clients.

---

## 8. Implementation phases

### Phase 0 — Schema and mock (1–2 days)

- [ ] Finalize JSON Schema files in repo (`schemas/game-event.json`, per-type refs).
- [ ] Add `publishGameEvent` + `EVENT_STREAM.mock = true` to `index.html`.
- [ ] Wire all six event hooks; verify via browser console during gameplay.
- [ ] Document example payloads (this proposal).

### Phase 1 — Local proxy + mock sink (2–3 days)

- [ ] Implement Node proxy with schema validation.
- [ ] Mock sink mode: append events to stdout / local JSONL file instead of Kafka.
- [ ] Point game at `http://localhost:3001/api/v1/events`.
- [ ] Test offline buffer and retry behavior (stop proxy, play, restart proxy).

### Phase 2 — Kafka integration (2–3 days)

- [ ] Connect proxy to dev Kafka cluster (or Confluent Cloud trial).
- [ ] Create topic `arcade.space-invaders.events` with retention and ACLs.
- [ ] Issue scoped tokens; deploy proxy to staging.
- [ ] Verify with `kafka-console-consumer`.

### Phase 3 — Production hardening (3–5 days)

- [ ] Rate limiting, monitoring (produce latency, error rate, buffer depth).
- [ ] Idempotency store (Redis).
- [ ] Dashboard or Grafana consumer for live event counts.
- [ ] Load test: 100 concurrent sessions × 5 events/min.

---

## 9. Testing

### 9.1 Local without Kafka

| Method | How |
|--------|-----|
| **Console mock** | `EVENT_STREAM.mock = true` — filter console for `[game-event]` |
| **Proxy mock sink** | `KAFKA_ENABLED=false` → proxy writes `events.jsonl` |
| **HTTP capture** | `npx mitmproxy` or browser DevTools Network tab on `POST /events` |

### 9.2 Manual gameplay checklist

| Action | Expected events |
|--------|-----------------|
| Start game | `lives.changed` (optional, reason `game_start`) |
| Shoot bunker | `bunker.friendly_fire` |
| Kill invader | `invader.killed`, `score.changed` |
| Get hit by invader bullet | `player.destroyed`, `lives.changed` |
| Clear all invaders | `level.completed` |
| Lose last life | `player.destroyed`, `lives.changed` (gameOver: true) |

### 9.3 Kafka verification

```bash
# Consume raw stream
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic arcade.space-invaders.events \
  --from-beginning \
  --property print.key=true \
  --property key.separator=":"

# Filter by session (partition key)
kafka-console-consumer ... | jq 'select(.sessionId=="<uuid>")'
```

### 9.4 Automated tests (future)

- **Unit:** Schema validation fixtures per event type.
- **Integration:** Playwright script drives game canvas keys, asserts proxy received N events.
- **Contract:** Consumer tests against golden JSONL fixture from Phase 0 mock runs.

---

## Appendix A — Event volume estimate

| Metric | Estimate |
|--------|----------|
| Events per invader kill | 2 (`invader.killed` + `score.changed`) |
| Events per player death | 2 (`player.destroyed` + `lives.changed`) |
| Events per level | 1 (`level.completed`) |
| Bunker hits | 0–20 per level (player skill dependent) |
| Peak rate per active player | ~5–15 events/minute |
| 100 concurrent players | ~500–1500 events/minute (~10–25/sec) |

Well within a single small proxy instance with batching.

---

## Appendix B — Files to add (implementation phase)

```
docs/space-invaders/
  index.html              # add publishGameEvent hooks
  kafka-streaming-proposal.md
services/event-proxy/
  package.json
  src/server.js
  src/schema.js
  schemas/
    game-event.json
    invader.killed.json
    ...
```

---

## Decision log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Transport | REST proxy | Static HTML, simplest ops |
| Topic name | `arcade.space-invaders.events` | Namespace + game + purpose |
| Partition key | `sessionId` | Per-session ordering |
| Client pattern | Fire-and-forget queue + batch | Protects frame loop |
| Auth | Scoped publish token | No secrets in browser |
