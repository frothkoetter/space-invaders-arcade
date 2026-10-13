# NiFi 2 — Space Invaders events → Iceberg (CDP / Cloudera)

End-to-end flow design for ingesting browser **ListenHTTP** JSON into an **Apache Iceberg** table queryable from **Trino**. Targets **NiFi 2.0+** (CDP DataFlow / Cloudera Flow Management) with the Iceberg NAR installed.

**Canonical full reference:** [nifi2-flow-full-description.md](./nifi2-flow-full-description.md) (processors, controller services, validation, operations).

**Primary ingest URL (Cloudera inbound):**

```text
http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

**projtriton inbound port 9088 is HTTP**, not HTTPS. Use `http://projtriton.inbound.dfx…:9088/space-invaders`. Using `https://` on 9088 fails at TLS handshake (curl error 35). Leave **SSL Context Service** empty on ListenHTTP for this port unless CDP docs for your tenant say otherwise.

**Game envelope** (from `publishGameEvent` in `index.html`):

| Field | Type | Notes |
|-------|------|--------|
| `eventId` | string (UUID) | Primary idempotency key |
| `eventType` | string | e.g. `invader.killed` |
| `occurredAt` | string (ISO-8601) | Event time |
| `sessionId` | string | Browser session |
| `playerId` | string | Stable anonymous id |
| `playerName` | string | Display name |
| `gameVersion` | string | Build tag |
| `level` | number | Game level |
| `frame` | number | Frame counter |
| `data` | object | Event-specific payload |

Bodies are either **one JSON object** or a **JSON array** of objects (batched, up to 50).

**Trino DDL companion:** [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl)

**Avro schemas:** [space-invaders-events.avsc](./space-invaders-events.avsc) (Iceberg/NiFi snake_case) · [space-invaders-events-wire.avsc](./space-invaders-events-wire.avsc) (browser camelCase wire JSON)

**ListenHTTP basics (CORS, curl):** [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md)

---

## Architecture overview

```text
┌─────────────┐     POST JSON      ┌──────────────┐     records     ┌─────────────────┐
│ Browser     │ ─────────────────► │ ListenHTTP   │ ───────────────►│ Parse / split   │
│ (game)      │   /space-invaders  │ :9088        │                 │ + validate      │
└─────────────┘                    └──────────────┘                 └────────┬────────┘
                                                                              │
                     ┌────────────────────────────────────────────────────────┘
                     ▼
              ┌──────────────┐     success      ┌──────────────────┐
              │ PutIceberg   │ ───────────────► │ Iceberg table    │
              │ Record       │                  │ (HMS / REST cat.)│
              └──────┬───────┘                  └──────────────────┘
                     │ failure / invalid
                     ▼
              ┌──────────────┐
              │ DLQ          │  (PutFile / PutS3Object / LogAttribute)
              └──────────────┘
```

Process group name suggestion: `space-invaders-ingest-iceberg`.

---

## Step 0 — Controller services (once per NiFi cluster or PG)

Create and **enable** these controller services before starting processors.

### 1. SSL Context Service (HTTPS ListenHTTP)

| Property | Value |
|----------|--------|
| **Type** | `StandardRestrictedSSLContextService` or CDP-provided SSL service |
| **Keystore** | Inbound / NiFi keystore per Cloudera docs |
| **Truststore** | Platform truststore |

Reference this service on **ListenHTTP** when listening with TLS on 9088 (or when NiFi sits behind inbound TLS termination — follow your CDP inbound pattern).

### 2. JSON Tree Record Reader

| Property | Value |
|----------|--------|
| **Type** | `JsonTreeReader` |
| **Schema Access Strategy** | `Infer Schema` or `Use 'Schema Text' Property` |
| **Schema Text** (if explicit) | Paste [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json) or [space-invaders-events-wire.avsc](./space-invaders-events-wire.avsc). Same **Avro record** shape as your working `marine_events` schema (`example.avro` namespace, `string` + `timestamp-millis` for timestamps). **`eventType` is `string`** (not Avro enum — dots in `invader.killed` are illegal). |
| **Date Format** | ISO-8601 for `occurredAt` |
| **Time Format** | ISO-8601 |
| **Timestamp Format** | ISO-8601 |

For production, prefer an explicit schema so `data` is treated consistently (string JSON vs map — see **UpdateRecord** below).

### 3. Iceberg Catalog Service

| Property | Example / placeholder |
|----------|------------------------|
| **Type** | `IcebergCatalogService` (NiFi 2 Iceberg bundle) |
| **Catalog Type** | `HIVE` (Hive Metastore) or `REST` (Iceberg REST catalog) |
| **Hive Metastore URI** | `thrift://YOUR_HMS_HOST:9083` (if `HIVE`) |
| **Catalog Name** | `YOUR_CATALOG` (e.g. `iceberg` — align with Trino catalog) |
| **Warehouse Location** | `YOUR_WAREHOUSE` (e.g. `s3a://bucket/warehouse` or `hdfs://…/warehouse`) |
| **Hadoop Configuration Resources** | `core-site.xml`, `hdfs-site.xml` (CDP classpath / conf dir) |
| **Kerberos** | Principal + keytab per CDP NiFi docs if cluster is secured |

**REST catalog alternative:** set catalog type to REST, **Catalog URI** to your REST endpoint, and warehouse per REST catalog config.

Enable the service only after HMS/REST connectivity is verified from the NiFi node.

---

## Step 1 — ListenHTTP

| Property | Value |
|----------|--------|
| **Listening Port** | `9088` |
| **Base Path** | `/space-invaders` |
| **HTTP Methods Allowed** | `POST` |
| **SSL Context Service** | Your SSL service (required for HTTPS inbound on Cloudera) |
| **Return Code** | `200` |
| **Max Data to Receive per Second** | Tune (e.g. 1–10 MB/s for arcade traffic) |
| **Authorized DN Pattern** | Empty for open ingest, or restrict if using client certs |

**Full URL for operators and game config:**

```text
http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

**Browser game setting:**

```javascript
localStorage.setItem('si-nifi', '1')
localStorage.setItem('si-nifi-listen-url', 'http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders')
```

Relationships: connect **success** downstream; route **failure** / **not found** to DLQ or retry policy (do not auto-terminate until DLQ exists).

---

## Step 2 — Route on method and content type

**Processor:** `RouteOnAttribute`

| Property | Value |
|----------|--------|
| **Routing Strategy** | `Route to Property name` |
| **Property: is_post_json** | `${http.method:equals('POST'):and(${mime.type:contains('json')})}` |

Adjust attribute names to your NiFi version (`mime.type`, `http.headers.content.type`, or ListenHTTP-specific attributes). Add a property **is_options** for CORS preflight if the inbound gateway forwards `OPTIONS` to NiFi.

| Relationship | Destination |
|--------------|-------------|
| **is_post_json** | Parse path |
| **unmatched** | DLQ (`LogAttribute` → `PutFile`) |

---

## Step 3 — Split batched arrays (optional but recommended)

The game may POST `[{...},{...}]`.

**Option A — SplitJson** (simple)

NiFi 2 may label this **JSON Paths** — you must **Add Property (+)** with path **`$.*`** (name is often `split`). Validation fails with *No JSON paths were specified* if none are added. See [nifi-validation-json-paths.md](./nifi-validation-json-paths.md).

| Property | Value |
|----------|--------|
| **JSON Paths** (dynamic) or **JsonPath Expression** | `$.*` |
| **Array Output Strategy** | `split` |

Connect **split** to the record path; **original** can go to DLQ if not an array/object edge case.

**Option B — Detect then split**

1. `EvaluateJsonPath` — set flowfile attribute `json.root` = `$[0]` vs `$.eventId` to detect array vs single object.
2. `RouteOnAttribute` — array → `SplitJson`; single → merge path.

---

## Step 4 — Parse and normalize records

### 4a. Convert to record stream

**Processor:** `ConvertRecord` (optional if downstream accepts raw JSON) or go straight to **PutIcebergRecord** with **JsonTreeReader**.

Typical chain:

```text
SplitJson → UpdateAttribute (mime.type = application/json)
         → UpdateRecord (rename to snake_case for Trino — see below)
         → PutIcebergRecord
```

### 4b. UpdateRecord (align with Trino DDL)

Map camelCase → snake_case and stringify `data` for a `VARCHAR` Iceberg column:

| Record field | New name |
|--------------|----------|
| `eventId` | `event_id` |
| `eventType` | `event_type` |
| `occurredAt` | `occurred_at` |
| `sessionId` | `session_id` |
| `playerId` | `player_id` |
| `playerName` | `player_name` |
| `gameVersion` | `game_version` |
| `level` | `level` |
| `frame` | `frame` |
| `data` | `data_json` (convert object → JSON string) |

**Processor:** `UpdateRecord`

| Property | Value |
|----------|--------|
| **Record Reader** | `JsonTreeReader` |
| **Record Writer** | `JsonRecordSetWriter` (or AvroRecordSetWriter matching Iceberg schema) |
| **Replacement Value Strategy** | `/event_id` ← `/eventId`, etc. |

Alternatively keep **camelCase** in Iceberg and match Trino column names in DDL — pick one convention; this doc assumes **snake_case** in the table ([trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl)).

### 4c. Add ingest metadata (optional)

**UpdateRecord** or **AttributesToJSON** + merge:

| Field | Source |
|-------|--------|
| `ingest_ts` | `${now():format('yyyy-MM-dd'T'HH:mm:ss.SSS'Z'','UTC')}` via record or attribute |

---

## Step 5 — Write to Iceberg

**Processor:** `PutIcebergRecord`

| Property | Value |
|----------|--------|
| **Record Reader** | `JsonTreeReader` (or writer from UpdateRecord chain) |
| **Iceberg Catalog Service** | Your enabled `IcebergCatalogService` |
| **Namespace** | `arcade` (Hive DB / Iceberg namespace) |
| **Table Name** | `space_invaders_events` |
| **File Format** | `PARQUET` |
| **Maximum File Size** | e.g. `128 MB` |
| **Maximum Records per File** | e.g. `500000` |
| **Write Format** | Append (default for event stream) |
| **Identifier Field** | `event_id` (if using Iceberg upsert / dedup — optional) |

Create the table first via Trino DDL (or allow `PutIcebergRecord` auto-create if your policy permits — prefer explicit DDL in production).

Relationships:

| Relationship | Destination |
|--------------|-------------|
| **success** | Terminate or metrics (`MonitorActivity`) |
| **failure** | DLQ pipeline |
| **retry** | `RetryFlowFile` → back to PutIcebergRecord (transient HMS/S3 errors) |

---

## Step 6 — Error handling (DLQ)

**Pattern:**

```text
failure / unmatched / parse errors
  → UpdateAttribute (dlq.reason = ${route.reason} or ${puticeberg.error})
  → AttributesToJSON (optional metadata wrapper)
  → PutFile
       Directory: /data/nifi-dlq/space-invaders/${now():format('yyyy-MM-dd')}
       Conflict Resolution: replace
```

Or **PutS3Object** to `s3a://YOUR_BUCKET/dlq/space-invaders/…` on CDP.

**LogAttribute** before PutFile: log `event_id`, `event_type`, `filename`, `uuid` at WARN for operations dashboards.

**ListenHTTP** failures: connect **failure** and **not found** to a small **LogAttribute** + PutFile DLQ so misconfigured clients are visible.

---

## Step 7 — Connect to your Iceberg catalog

| Catalog style | NiFi `IcebergCatalogService` | Trino |
|---------------|------------------------------|--------|
| **Hive Metastore** | Catalog Type `HIVE`, URI `thrift://…:9083`, warehouse `YOUR_WAREHOUSE` | `iceberg` catalog pointing at same HMS |
| **REST** | Catalog Type `REST`, REST URI, warehouse | Trino `iceberg_rest` connector matching REST |

Placeholders to replace in CM / Ambari / env config:

- `YOUR_CATALOG` — logical catalog name (e.g. `iceberg`)
- `YOUR_WAREHOUSE` — root warehouse path for Iceberg files
- `YOUR_HMS_HOST` — Hive Metastore host for thrift URI

After deploy, run the Trino DDL once, then smoke-test with curl (below) and `SELECT count(*) FROM iceberg.arcade.space_invaders_events`.

---

## Processor chain summary

| # | Processor | Purpose |
|---|-----------|---------|
| 1 | **ListenHTTP** | Ingest POST on `:9088` `/space-invaders` |
| 2 | **RouteOnAttribute** | POST + JSON only |
| 3 | **SplitJson** | One FlowFile per event when body is array |
| 4 | **UpdateRecord** | snake_case + `data_json`; optional `ingest_ts` |
| 5 | **PutIcebergRecord** | Append to `arcade.space_invaders_events` |
| 6 | **PutFile** (DLQ) | Invalid requests / write failures |

Controller services: **SSL Context**, **JsonTreeReader**, **IcebergCatalogService** (+ **JsonRecordSetWriter** if used in UpdateRecord).

---

## Verify with curl

Single event:

```bash
curl -sS -X POST \
  'http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders' \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "00000000-0000-4000-8000-000000000001",
    "eventType": "invader.killed",
    "occurredAt": "2026-10-12T12:00:00.000Z",
    "sessionId": "curl-smoke",
    "playerId": "anon-curl",
    "playerName": "PILOT",
    "gameVersion": "flag-edition-v15-nifi-listenhttp",
    "level": 1,
    "frame": 100,
    "data": { "pointsAwarded": 10, "row": 0, "col": 3 }
  }'
```

Add `-k` only if you must accept an untrusted cert in a lab (not for production).

Batch (game-style):

```bash
curl -sS -X POST \
  'http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders' \
  -H 'Content-Type: application/json' \
  -d '[{"eventId":"a","eventType":"score.changed","occurredAt":"2026-10-12T12:00:01.000Z","sessionId":"s","playerId":"p","playerName":"P","gameVersion":"flag-edition-v15-nifi-listenhttp","level":1,"frame":1,"data":{"score":10}},{"eventId":"b","eventType":"invader.killed","occurredAt":"2026-10-12T12:00:02.000Z","sessionId":"s","playerId":"p","playerName":"P","gameVersion":"flag-edition-v15-nifi-listenhttp","level":1,"frame":2,"data":{}}]'
```

Trino check:

```sql
SELECT event_type, count(*) AS n
FROM iceberg.arcade.space_invaders_events
WHERE occurred_at >= current_timestamp - interval '1' hour
GROUP BY 1;
```

---

## NiFi 2 / CDP notes

- Requires **NiFi 2.0+** and the **Iceberg** NAR (bundled in CDP DataFlow 7.x+; confirm in **Controller Settings → NARs**).
- Processor names are stable in Apache NiFi 2: `ListenHTTP`, `RouteOnAttribute`, `SplitJson`, `UpdateRecord`, `ConvertRecord`, `PutIcebergRecord`, `IcebergCatalogService`, `JsonTreeReader`.
- Cloudera inbound hostname **`projtriton.inbound.dfx…`** routes to your tenant ListenHTTP — port **9088** and path **`/space-invaders`** must match this flow and the game’s `si-nifi-listen-url`.
- Browser **CORS** still applies; see [nifi-listenhttp-setup.md](./nifi-listenhttp-setup.md). Production often uses same-origin proxy or gateway CORS in front of inbound.
- **NiFi flow template checklist:** [nifi-flow-template-notes.md](./nifi-flow-template-notes.md)

---

## Game configuration reference

| Key | CDP example value |
|-----|-------------------|
| `si-nifi` | `1` |
| `si-nifi-listen-url` | `http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders` |

Local dev default in `index.html` remains `http://127.0.0.1:9998/space-invaders` unless overridden in localStorage.
