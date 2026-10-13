# NiFi — Fix: “JSON Paths is invalid — No JSON paths were specified”

This validation error on **Perform Validation** (e.g. `dfx-nifi-0…:8443`) almost always means a processor or controller service is set to use **JSON Paths** but no path was entered.

---

## Case 1 — JsonTreeReader (most common)

If **Schema Access Strategy** = **Use JSON Paths** (or similar), you **must** add at least one JSON Path. For Space Invaders, **do not use that strategy**.

**Recommended settings**

| Property | Value |
|----------|--------|
| **Schema Access Strategy** | **Use 'Schema Text' Property** |
| **Schema Text** | Paste contents of [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json) |
| **Schema Text** (alt) | Or **Infer Schema** for first tests (no Schema Text) |

Leave **JSON Paths** empty / unused when not on “Use JSON Paths” strategy.

**Date / time formats:** ISO-8601 for `occurredAt`.

---

## Case 2 — SplitJson processor

NiFi 2 **SplitJson** requires at least one JSON path (dynamic property or expression, depending on version).

**To split batched game arrays `[{...},{...}]`:**

1. Open **SplitJson** → **Add Property** (+) under **JSON Paths** (or set **JsonPath Expression** if your build shows that name).
2. Add:

| Property name (label) | JSON Path value |
|----------------------|-----------------|
| `split` (or default relationship name your UI shows) | `$.*` |

3. **Array Output Strategy:** `split` (when available).

**Single-object POST only:** you can **bypass SplitJson** — wire ListenHTTP straight to UpdateRecord / ConvertRecord / PutIcebergRecord.

---

## Case 3 — EvaluateJsonPath

Each extracted field is a **dynamic property**:

| Property name (= attribute name) | Value |
|----------------------------------|--------|
| `eventId` | `$.eventId` |
| `eventType` | `$.eventType` |

If you add **EvaluateJsonPath** with zero dynamic properties, validation fails with the same class of error.

---

## Minimal flow without SplitJson (fastest validation)

```text
ListenHTTP → RouteOnAttribute (POST + JSON) → UpdateRecord → PutIcebergRecord
```

Use **JsonTreeReader** on PutIcebergRecord with **Schema Text** from `nifi-jsontree-schema-text.json`. Handle `data` object via **UpdateRecord** `/data` → `/data_json` as JSON string (see [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md)).

---

## After changes

1. **Apply** processor / controller service.
2. **Perform Validation** again on the process group or canvas.
3. **Start** ListenHTTP only after controller services (JsonTreeReader, IcebergCatalogService, SSL) are **Enabled**.
