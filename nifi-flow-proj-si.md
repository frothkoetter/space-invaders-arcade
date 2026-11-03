# NiFi flow `proj_si` (exported template)

CDP DataFlow process group **`proj_si`**: game **ListenHTTP** ingest → **Iceberg** + **Kafka** in parallel.

**Import file:** [nifi-flows/proj_si-version-1.json](./nifi-flows/proj_si-version-1.json)  
(NiFi 2 **Upload flow definition** / import; requires Iceberg + CDF Kafka NARs **2.6.0.4.12.0.2-1** or compatible.)

---

## Architecture

```text
                    ┌─────────────────┐
  POST JSON         │   ListenHTTP    │  Base Path: space-invaders
  /space-invaders   │   Port: #{...}  │  Return Code: 200
                    └────────┬────────┘
                             │ success
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌────────────────┐           ┌─────────────────────────┐
     │  MergeContent  │           │ PublishKafka2RecordCDP  │
     │  (batch/merge) │           │ Topic: space_invaders_  │
     └───────┬────────┘           │        events           │
             │ merged             └───────────┬─────────────┘
             ▼                                │ success (auto-term)
     ┌────────────────┐                     │ failure → funnel
     │   PutIceberg   │                     └──────────────► DLQ funnel
     │ Catalog NS:    │
     │  space_invaders│
     │ Table:         │
     │  space_invaders│
     │  _events       │
     └───────┬────────┘
             │ success (auto-term)
             │ failure → funnel
```

---

## Parameter context `proj_si`

| Parameter | Purpose |
|-----------|---------|
| `Listening Port` | ListenHTTP port (default **9088** in export) |
| `Kafka Brokers` | CDP Kafka bootstrap servers |
| `cdp_workload_user` | SASL / Kerberos principal for Kafka + Iceberg |
| `cdp_workload_password` | Sensitive — set in NiFi UI after import, not in git |
| `CDPEnvironment` | Hadoop config resources for **HiveCatalogService** |

After import, set **`cdp_workload_password`** and **`CDPEnvironment`** asset paths for your cluster.

---

## PutIceberg (Iceberg landing)

| Property | Value |
|----------|--------|
| **Catalog Service** | `HiveCatalogService` |
| **Catalog Namespace** | **`space_invaders`** |
| **Table Name** | **`space_invaders_events`** |
| **Record Reader** | `JsonTreeReader-space_invaders` |

Matches Trino/Hive DDL: **`iceberg.space_invaders.space_invaders_events`** — [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl).

> **Fix applied in repo export:** original download had `catalog-namespace` = `spaceinvader`; template in git uses **`space_invaders`**.

---

## PublishKafka2RecordCDP (Flink / SSB source)

| Property | Value |
|----------|--------|
| **Topic** | `space_invaders_events` |
| **Security** | SASL_SSL / PLAIN |
| **Record Reader / Writer** | Same JsonTreeReader + JsonRecordSetWriter |

Feeds Flink table **`ssb.ssb_default.space_invaders_events`** — see [flink-kafka-data-source.md](./flink-kafka-data-source.md) and [flink-top10player-mv.sql](./flink-top10player-mv.sql).

---

## Controller services (enable after import)

- **JsonTreeReader-space_invaders** — Avro schema record `space_invaders_events` (epoch-ms `occurred_at` / `ingest_ts`)
- **JsonRecordSetWriter**
- **HiveCatalogService** — `hadoop-config-resources` = `#{CDPEnvironment}`
- **KerberosPasswordUserService** — `#{cdp_workload_user}` / `#{cdp_workload_password}`
- **Default NiFi SSL Context Service** (external) — Kafka SSL

Schema text aligns with [nifi-jsontree-schema-text.json](./nifi-jsontree-schema-text.json).

---

## Import steps

1. Create or select parameter context **`proj_si`** (or map parameters on import).
2. **Import** [proj_si-version-1.json](./nifi-flows/proj_si-version-1.json) into NiFi 2.
3. Enable controller services; set **CDPEnvironment** and **cdp_workload_password**.
4. Confirm Iceberg table exists (`space_invaders.space_invaders_events`).
5. Start **ListenHTTP** last; test with [curl-smoke-test.sh](./curl-smoke-test.sh) or the game with `nifi-url` pointing at inbound **HTTP** `:9088`.

---

## Related

- [nifi-flow-template-notes.md](./nifi-flow-template-notes.md) — generic checklist  
- [nifi-iceberg-ingest-flow.md](./nifi-iceberg-ingest-flow.md) — design reference  
- [enable-nifi-events.md](./enable-nifi-events.md) — browser ingest URL  
