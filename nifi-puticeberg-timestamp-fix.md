# PutIceberg — timestamp conversion error

## Error

```text
MalformedRecordException: ... failed to convert into a Record object with the given schema
FieldConversionException: Conversion failed for [2026-10-12T12:00:00.000Z] named [occurred_at] to [java.time.LocalDateTime]
NumberFormatException: For input string: "2026-10-12T12:00:00.000Z"
```

## What it means

The FlowFile **content** still has **`occurred_at` as an ISO string** (with `Z`), for example:

```json
"occurred_at": "2026-10-12T12:00:00.000Z"
```

PutIceberg + Hive **`TIMESTAMP`** expect **`occurred_at` as epoch milliseconds** (JSON **number**, Avro **`long`** + `timestamp-millis`). Schema-only changes do not fix **old JSON already in the queue**.

## Fix A — Send millis (preferred)

1. **JsonTreeReader → Schema Text:** paste [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json) (`occurred_at` = **`long`**, not string).
2. **Schema Access Strategy:** `Use 'Schema Text' Property` (not **Infer Schema**).
3. **Empty queues** upstream of PutIceberg (List Queue → drop old FlowFiles with ISO strings).
4. POST **numeric** millis (no quotes):

   ```json
   "occurred_at": 1791806400000
   ```

5. **Verify:** Provenance → View Data on the connection **into** PutIceberg — `occurred_at` must be a **number**, not `"2026-10-12..."`.

**Curl:** [curl-smoke-test.sh](./curl-smoke-test.sh) · **Game:** `index.html` v17 (`occurred_at: Date.now()`).

## Fix B — NiFi converts ISO → millis (legacy payloads)

Insert **InvokeScriptedRecordTransform** (or **ScriptedTransformRecord** on your NiFi build) **before** PutIcebergRecord:

| Property | Value |
|----------|--------|
| **Record Reader** | Same JsonTreeReader |
| **Record Writer** | JsonRecordSetWriter with [space-invaders-events.avsc](./space-invaders-events.avsc) |
| **Script Engine** | Groovy |
| **Script Body** | [nifi-record-iso-to-millis.groovy](./nifi-record-iso-to-millis.groovy) |

Wire: `ListenHTTP → … → InvokeScriptedRecordTransform → PutIcebergRecord`.

## ingest_ts

Omit in POST; optional **UpdateRecord** before PutIceberg:

| Property | Value | Strategy |
|----------|--------|----------|
| `/ingest_ts` | `${now():toNumber()}` | Expression Language Value |

## Hive DDL

Keep `occurred_at TIMESTAMP` / `ingest_ts TIMESTAMP` — no DDL change when millis are used.
