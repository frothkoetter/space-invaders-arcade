# NiFi — Wire JSON → Iceberg `space_invaders_events`

## Symptom

**PutIcebergRecord** queue FlowFile content looks like:

```json
{
  "eventId": "...",
  "eventType": "invader.killed",
  "data": { "pointsAwarded": 10 }
}
```

Hive / Iceberg table columns are **snake_case** and **`data_json` is STRING**, not a nested object:

```sql
event_id, event_type, occurred_at, …, data_json, ingest_ts
```

PutIceberg matches **record field names** to **table columns**. CamelCase + `data` object → no matching columns → failure or empty writes.

---

## Fix (required): **UpdateRecord** before **PutIcebergRecord**

```text
ListenHTTP → RouteOnAttribute → (SplitJson) → UpdateRecord → PutIcebergRecord
```

Do **not** connect ListenHTTP / SplitJson **success** directly to PutIcebergRecord.

---

## Controller services (two readers or reader + writer)

| Service | Schema Text |
|---------|-------------|
| **JsonTreeReader** (wire) | [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json) — camelCase |
| **JsonRecordSetWriter** (iceberg row) | [space-invaders-events.avsc](./space-invaders-events.avsc) — snake_case |

Optional second **JsonTreeReader** (iceberg) with same **space-invaders-events.avsc** on **PutIcebergRecord** only.

**JsonTreeReader (wire):** Schema Access Strategy = **Use 'Schema Text' Property** (not empty **JSON Paths**).

---

## UpdateRecord settings

| Property | Value |
|----------|--------|
| **Record Reader** | JsonTreeReader (wire / camelCase) |
| **Record Writer** | JsonRecordSetWriter (snake_case Avro) |
| **Replacement Value Strategy** | Per row below |

Add **dynamic properties** (RecordPath → value). Use **RecordPath Value** unless noted.

| Property (field to set) | Value | Strategy |
|-------------------------|-------|----------|
| `/event_id` | `/eventId` | RecordPath Value |
| `/event_type` | `/eventType` | RecordPath Value |
| `/occurred_at` | `/occurredAt` | RecordPath Value |
| `/session_id` | `/sessionId` | RecordPath Value |
| `/player_id` | `/playerId` | RecordPath Value |
| `/player_name` | `/playerName` | RecordPath Value |
| `/game_version` | `/gameVersion` | RecordPath Value |
| `/level` | `/level` | RecordPath Value |
| `/frame` | `/frame` | RecordPath Value |
| `/data_json` | see below | Expression or Jolt |
| `/ingest_ts` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","UTC")}` | Expression Language Value |

### Stringify `data` → `data_json`

Pick one:

**A — JoltTransformJSON** (before UpdateRecord, on FlowFile content):

```json
[
  {
    "operation": "shift",
    "spec": {
      "eventId": "event_id",
      "eventType": "event_type",
      "occurredAt": "occurred_at",
      "sessionId": "session_id",
      "playerId": "player_id",
      "playerName": "player_name",
      "gameVersion": "game_version",
      "level": "level",
      "frame": "frame",
      "data": "data_obj"
    }
  },
  {
    "operation": "modify-overwrite-beta",
    "spec": {
      "data_json": "=toJson(@(1,data_obj))",
      "data_obj": null
    }
  },
  {
    "operation": "modify-overwrite-beta",
    "spec": {
      "ingest_ts": "=now()"
    }
  }
]
```

Then **UpdateRecord** only adds `ingest_ts` if needed, or skip UpdateRecord field renames if Jolt already outputs snake_case.

**B — UpdateRecord only:** If your NiFi build supports record functions on `/data_json`, serialize `/data` to JSON string per CDP docs; otherwise use **JoltTransformJSON** (recommended).

---

## PutIcebergRecord settings

| Property | Value |
|----------|--------|
| **Record Reader** | JsonTreeReader with **space-invaders-events.avsc** (snake_case), **or** reader that matches UpdateRecord output |
| **Iceberg Catalog Service** | Enabled |
| **Namespace / Database** | Your Hive DB (e.g. `default` or `arcade`) |
| **Table Name** | **`space_invaders_events`** |
| **Unmatched Field Behavior** | `Ignore Unmatched Fields` (after mapping) or `Fail` while debugging |

After **UpdateRecord**, FlowFile content should look like:

```json
{
  "event_id": "00000000-0000-4000-8000-000000000001",
  "event_type": "invader.killed",
  "occurred_at": "2026-10-12T12:00:00.000Z",
  "session_id": "curl-smoke",
  "player_id": "anon-curl",
  "player_name": "PILOT",
  "game_version": "flag-edition-v15-nifi-listenhttp",
  "level": 1,
  "frame": 100,
  "data_json": "{\"pointsAwarded\":10}",
  "ingest_ts": "2026-10-12T12:05:00.000Z"
}
```

Use **LogAttribute** or **Provenance → View Data** on the connection **into** PutIcebergRecord to confirm.

---

## Table DDL alignment

Your DDL uses `TIMESTAMP` for `occurred_at` / `ingest_ts`. NiFi may send ISO strings; Iceberg/Trino often coerce if types match catalog. If timestamps fail, set **UpdateRecord** to normalize format or use logicalType in Avro ([space-invaders-events.avsc](./space-invaders-events.avsc)).

---

## Quick checklist

- [ ] UpdateRecord (or Jolt + UpdateRecord) **between** ingest and PutIcebergRecord
- [ ] Queue **before** PutIceberg shows **snake_case** + **data_json** string
- [ ] PutIceberg **Namespace** + **Table Name** match `CREATE TABLE … space_invaders_events`
- [ ] JsonTreeReader not on **Use JSON Paths** with empty paths
