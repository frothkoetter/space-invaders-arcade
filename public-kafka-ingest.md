# Space Invaders — Public Kafka ingest (browser → HTTPS → Kafka)

> **Superseded:** Kafka ingest and `event-proxy/` were removed in **flag-edition-v13-tcp-events**. Use **[tcp-forward/](./tcp-forward/)** and [README.md](./README.md) (TCP JSON-lines via local HTTP forwarder). This document is kept for historical reference only.

**Audience:** Players on the public internet (no VPN, no local Node bridge on their device).  
**Game build:** `flag-edition-v12-public-ingest` (obsolete)  
**Related:** [kafka-streaming-proposal.md](./kafka-streaming-proposal.md) — event schema still applies; transport changed to TCP.

---

## Overview

```
[Static game HTML/JS]  --HTTPS POST /events-->  [Hosted ingest service]  --SASL_SSL-->  [Cloudera Kafka :9093]
        |                                              |
   si-kafka-ingest-url                          KAFKA_* in env / secrets
   si-kafka-ingest-key (header)                 INGEST_API_KEY (never in browser as Kafka password)
```

The browser never holds Kafka SASL credentials. It only knows a **public ingest base URL** and an optional **ingest API key** (scoped publish secret, rotatable).

---

## Static game hosting

The game is a single `index.html` (plus optional assets). Any static host works:

| Option | Notes |
|--------|--------|
| **Cloudflare Pages / GitHub Pages** | Free HTTPS, good for demos; set CORS on ingest to your Pages origin |
| **Amazon S3 + CloudFront** | Versioned deploys; origin = CloudFront domain in `CORS_ORIGINS` |
| **nginx / object storage** | Serve `docs/space-invaders/` as root; enable gzip for large optional bundles |

**Client config** (localStorage, or query params once):

```javascript
localStorage.setItem('si-kafka', '1');
localStorage.setItem('si-kafka-ingest-url', 'https://ingest.example.com/events');
localStorage.setItem('si-kafka-ingest-key', 'YOUR_ROTATABLE_INGEST_KEY');
```

Optional console mock: `localStorage.setItem('si-events', '1')`.

Local dev unchanged: omit `si-kafka-ingest-url` and run `event-proxy` on the Mac at `http://localhost:3001/events`.

---

## Hosted ingest service

Same Node app as the local bridge (`event-proxy/`), packaged for a container or VM:

- **Process:** Express-less `http` server; `POST /events`, `GET /health`
- **Secrets (env):** `KAFKA_BROKERS`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`, optional `KAFKA_SASL_MECHANISM`, `KAFKA_TOPIC`
- **Public controls (env):** `INGEST_API_KEY`, `CORS_ORIGINS`, `PORT`, `LISTEN_HOST=0.0.0.0`
- **Rate limits:** in-process per-IP window (see server); add edge rate limiting (CDN/WAF) in production

Build and run locally (outline):

```bash
cd docs/space-invaders/event-proxy
docker build -t space-invaders-ingest .
docker run --env-file .env -p 3001:3001 space-invaders-ingest
```

Deploy target examples: ECS/Fargate, GKE, Cloud Run (with VPC connector), single VM behind an ALB — one or more replicas behind a load balancer.

---

## Security model

| Layer | Mechanism |
|-------|-----------|
| **Kafka credentials** | Server env only; never embedded in `index.html` or localStorage |
| **Browser → ingest** | Optional `INGEST_API_KEY` on server; client sends `X-Ingest-Key: …` or `Authorization: Bearer …` |
| **CORS** | `CORS_ORIGINS` comma-separated allowlist; reflects matching `Origin` — not a substitute for API key + rate limits |
| **Rate limiting** | Server: default 100 req/min per IP on `POST /events`; tune via env; add CDN/WAF for abuse |
| **Payload validation** | Event schema checks server-side; reject unknown `eventType` |
| **Errors** | Generic 502 to client; broker hostnames not echoed in responses |

**Do not** ship the ingest API key in a public repo or in query strings (use localStorage after redirect, or a short-lived token endpoint later).

For **demos**, a shared ingest key is acceptable if rotated frequently and rate-limited. For **production**, prefer per-session tokens from `POST /auth/session` (future) or an API gateway.

---

## Network: DNS, TLS, and Kafka reachability

1. **DNS:** `ingest.example.com` → load balancer → ingest containers.
2. **TLS:** Terminate HTTPS at the load balancer or ingress (ACM/Let’s Encrypt). Container may listen HTTP on port 3001 internally.
3. **Kafka brokers:** Cloudera sandbox brokers are typically **private** (`*.cloudera.site:9093`). The ingest service must run where it can open **outbound TCP 9093** with SASL_SSL:
   - **Inside the same VPC / peered network** as the cluster, or
   - **VPN** from the ingest host to Cloudera (common for sandbox on a developer laptop; **not** on player devices).

Players only need HTTPS to the ingest URL. **Only the ingest fleet** needs VPN/VPC path to brokers.

---

## Environment variables (ingest server)

| Variable | Required | Purpose |
|----------|----------|---------|
| `KAFKA_BROKERS` | yes | Comma-separated bootstrap hosts |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | yes | SASL credentials |
| `KAFKA_TOPIC` | no | Default `arcade.space-invaders.events` |
| `KAFKA_SASL_MECHANISM` | no | `plain`, `scram-sha-256`, etc. |
| `PORT` | no | Default `3001` |
| `LISTEN_HOST` | no | `0.0.0.0` for containers; `127.0.0.1` for local-only |
| `INGEST_API_KEY` | recommended public | If set, all `POST /events` must present key |
| `CORS_ORIGINS` | recommended public | Comma-separated browser origins |
| `CORS_ORIGIN` | legacy single | Used if `CORS_ORIGINS` empty |
| `RATE_LIMIT_PER_MINUTE` | no | Per-IP cap on POST (default 100) |
| `EVENT_JSONL` | no | Optional audit file |

See [event-proxy/.env.example](./event-proxy/.env.example).

---

## Deploy outline (no cloud execution in repo)

1. **Provision** ingest runtime in a network that reaches Kafka `:9093` (VPC + optional VPN from ops, not players).
2. **Store secrets** in a secret manager; inject as env at deploy time.
3. **Set** `INGEST_API_KEY`, `CORS_ORIGINS=https://arcade.example.com`, `LISTEN_HOST=0.0.0.0`.
4. **Build/push** image from `event-proxy/Dockerfile`; deploy 1+ tasks behind HTTPS LB.
5. **Health check:** `GET /health` → `{ ok: true }`.
6. **Publish game** to static hosting; configure players with ingest URL + key (see README).
7. **Verify:** play in browser; consume topic on a Kafka client inside the trusted network.

---

## Client localStorage (production example)

```javascript
localStorage.setItem('si-events', '1'); // optional DevTools mock
localStorage.setItem('si-kafka', '1');
localStorage.setItem('si-kafka-ingest-url', 'https://ingest.example.com/events');
localStorage.setItem('si-kafka-ingest-key', '<rotate-me-ingest-api-key>');
location.reload();
```

Console should show: `[kafka] using public HTTPS ingest` (not the local Mac bridge hint).

---

## Decision log

| Decision | Choice |
|----------|--------|
| Player transport | HTTPS POST to hosted ingest |
| Kafka auth | SASL only on ingest server |
| Browser secret | Ingest API key header, not Kafka password |
| Local dev | Same code; no `si-kafka-ingest-url` → `localhost:3001` |
