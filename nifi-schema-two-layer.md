# NiFi — Schema alignment (game, curl, Avro, Hive)

**Current (v16+):** One shape everywhere — snake_case fields and `data_json` string. See [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json). NiFi can go **ListenHTTP → PutIcebergRecord** with the same Schema Text (optional **UpdateRecord** only for `ingest_ts`).

---

## Legacy — Two Avro schemas (wire struct → flat table row)

## Problem

`SpaceInvaderEventWire` (camelCase, no `data`) does not match:

```sql
space_invaders_events (
  event_id STRING, event_type STRING, occurred_at TIMESTAMP,
  session_id STRING, player_id STRING, player_name STRING,
  game_version STRING, level INT, frame BIGINT,
  data_json STRING, ingest_ts TIMESTAMP
)
```

## Files

| File | When to paste in **JsonTreeReader → Schema Text** |
|------|---------------------------------------------------|
| [nifi-jsontree-wire-schema-text.json](./nifi-jsontree-wire-schema-text.json) | After **ListenHTTP** — reads curl/game JSON; **`data` is a nested struct** |
| [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json) | Before **PutIcebergRecord** — **flat snake_case** = table columns |

Use **two** controller services (or change Schema Text between stages):

- `JsonTreeReader-Wire` → wire schema  
- `JsonTreeReader-Iceberg` → table schema  

**Schema Access Strategy:** `Use 'Schema Text' Property` (not empty **JSON Paths**).

## Flow

```text
ListenHTTP
  → UpdateRecord (Reader: Wire, Writer: table Avro)
       map /eventId → /event_id, … ; /data → serialize → /data_json ; /ingest_ts
  → PutIcebergRecord (Reader: Iceberg / table schema)
```

Or **JoltTransformJSON** (rename + `toJson` on `data`) then **UpdateRecord** for `ingest_ts` only.

## Hive / Iceberg column ↔ Avro (table schema)

| Column | Avro field | Type |
|--------|------------|------|
| `event_id` | `event_id` | string |
| `event_type` | `event_type` | string |
| `occurred_at` | `occurred_at` | string + `timestamp-millis` |
| `session_id` | `session_id` | string |
| `player_id` | `player_id` | string |
| `player_name` | `player_name` | string |
| `game_version` | `game_version` | string |
| `level` | `level` | int |
| `frame` | `frame` | long |
| `data_json` | `data_json` | string (JSON text of `data` struct) |
| `ingest_ts` | `ingest_ts` | null or string + `timestamp-millis` |

Sync from Agent Store Context if your Mac still shows old `SpaceInvaderEventWire` / `arcade.spaceinvaders`.
