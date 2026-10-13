# NiFi 2 flow checklist — Space Invaders → Iceberg

Quick deployment checklist for **NiFi 2.0+** (CDP DataFlow). Full steps: [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md).

## Ingest URL

```text
http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders
```

Host: `projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site` · Port: **9088** · Base path: **`/space-invaders`**

## Controller services

- [ ] `StandardRestrictedSSLContextService` (or CDP SSL) — referenced by ListenHTTP if TLS on NiFi
- [ ] `JsonTreeReader` — infer or explicit schema for game envelope
- [ ] `JsonRecordSetWriter` — if using UpdateRecord before PutIcebergRecord
- [ ] `IcebergCatalogService` — `YOUR_CATALOG`, `YOUR_WAREHOUSE`, HMS thrift or REST URI

## Processors (in order)

- [ ] **ListenHTTP** — port `9088`, base path `/space-invaders`, POST, return `200`
- [ ] **RouteOnAttribute** — POST + JSON → main; else → DLQ
- [ ] **SplitJson** — `$.*` when body is batched array
- [ ] **UpdateAttribute** — `mime.type=application/json` (if needed)
- [ ] **UpdateRecord** — camelCase → snake_case; `data` → `data_json` string
- [ ] **PutIcebergRecord** — namespace `arcade`, table `space_invaders_events`, PARQUET
- [ ] **PutFile** (DLQ) — `/data/nifi-dlq/space-invaders/` or S3 prefix

## Relationships / error handling

- [ ] ListenHTTP **failure**, **not found** → DLQ
- [ ] PutIcebergRecord **failure** → DLQ (+ optional **RetryFlowFile** for retry)
- [ ] RouteOnAttribute **unmatched** → DLQ
- [ ] No auto-terminate on failure paths until DLQ verified

## Downstream

- [ ] Trino DDL applied: [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl)
- [ ] curl smoke test (single object + array)
- [ ] Game: `si-nifi-listen-url` set to **HTTP** URL above; CORS or proxy if browser cross-origin

## Export template (optional)

When your NiFi UI supports export, save the process group as `space-invaders-ingest-iceberg.xml` and store outside git if it contains secrets. Re-import requires the same NAR versions (Iceberg bundle on NiFi 2).

## Version note

Document written for **Apache NiFi 2.x** processor names (`PutIcebergRecord`, `IcebergCatalogService`). NiFi 1.x used different integration patterns; upgrade to NiFi 2 before deploying this checklist.
