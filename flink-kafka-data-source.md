# Flink SSB — Kafka data source (`local`)

Register the CDP Kafka cluster **once** in **Streaming SQL Studio** before you create the **`space_invaders_events`** Kafka table or run [flink-top10player-mv.sql](./flink-top10player-mv.sql).

The data source name **`local`** is what the Kafka Table wizard shows as **Kafka Cluster** (see screenshots in repo docs / your SSB UI).

---

## Step 0 — Create Kafka data source (UI)

1. Open **Streaming SQL Studio** → **Data Sources** (or **Kafka Data Sources**).
2. **Add** → **Kafka Data Source**.
3. Set fields to match your CDP Kafka brokers (SASL/SSL):

| Field | Value |
|-------|--------|
| **Name** | `local` |
| **Brokers** | `proj-si-master1.se-sandb.a465-9q4k.cloudera.site:9093, proj-si-master0.se-sandb.a465-9q4k.cloudera.site:9093` |
| **Protocol** | `SASL/SSL` |
| **Use default TrustStore** | **On** (Cloudera agent cert: `cm-auto-global_truststore.jks`) |
| **Kafka KeyStore** | *(leave empty unless your tenant requires client certs)* |
| **SASL Mechanism** | `PLAIN` |
| **SASL Username** | Your CDP workload user (e.g. `frothkoetter`) |
| **SASL Password** | Your CDP password *(never commit to git)* |

4. Save the data source.

**TrustStore path** (when not using “default” in custom setups):

```text
/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
```

---

## Step 1 — Kafka table (UI, recommended)

1. **Tables** → **Add Table** → **Kafka Table**.
2. Configure:

| Field | Value |
|-------|--------|
| **Table Name** | `space_invaders_events` |
| **Kafka Cluster** | `local` *(the data source from Step 0)* |
| **Data Format** | `JSON` |
| **Topic Name** | `space_invaders_events` |

3. **Schema Definition** → **Detect Schema** (or paste schema from [flink-top10player-mv.sql](./flink-top10player-mv.sql)).
4. **Event Time** → add **`eventTimestamp`** from Kafka metadata if not auto-added; watermark **3 seconds** (matches game script).
5. **Create**.

Qualified name in SSB is typically:

```text
`ssb`.`ssb_default`.`space_invaders_events`
```

---

## Equivalent connector properties (SQL DDL)

If you create the table with **SQL** instead of the wizard, the **`WITH (...)`** clause must carry the same settings as data source **`local`**. See **Step 1** in [flink-top10player-mv.sql](./flink-top10player-mv.sql).

Mapping from UI → Flink Kafka connector:

| SSB data source (UI) | SQL `WITH` property |
|----------------------|---------------------|
| Brokers | `properties.bootstrap.servers` |
| SASL/SSL | `properties.security.protocol` = `SASL_SSL` |
| SASL PLAIN | `properties.sasl.mechanism` = `PLAIN` |
| Username / password | `properties.sasl.jaas.config` = `... username="..." password="...";` |
| Default truststore | `properties.ssl.truststore.location` + `properties.ssl.truststore.password` |
| Topic | `topic` = `space_invaders_events` |
| JSON payloads | `format` = `json` |
| Read from beginning (dev) | `scan.startup.mode` = `earliest-offset`, `properties.auto.offset.reset` = `earliest` |

Example JAAS line (replace credentials):

```text
org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_CDP_USER" password="YOUR_CDP_PASSWORD";
```

---

## Step 2 — Materialized view

After the Kafka table exists, run **`CREATE MATERIALIZED VIEW top10player`** from [flink-top10player-mv.sql](./flink-top10player-mv.sql) (Step 2): **HOP** window **1 hour** lookback, **1 minute** slide, top 10 by max score.

---

## Related

- [top10-leaderboard.md](./top10-leaderboard.md) — browser UI + SSB proxy
- [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl) — Iceberg landing (NiFi), optional parallel to Kafka
