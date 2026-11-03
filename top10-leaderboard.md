# Top 10 pilots leaderboard (Flink SSB)

Live ranking UI: [top10-leaderboard.html](./top10-leaderboard.html)

Data source: Flink SSB materialized view **`top10player`** on the trition-si gateway — **1-hour sliding window**, **1-minute slide** (updates every minute using all `score.changed` events in the trailing hour).

**Primary Flink source:** Kafka virtual table **`ssb`.`ssb_default`.`space_invaders_events`** (topic **`space_invaders_events`**, JSON).  
**Iceberg landing zone (NiFi):** **`iceberg.space_invaders.space_invaders_events`** — [trino-iceberg-space-invaders.ddl](./trino-iceberg-space-invaders.ddl).

## Kafka data source + table (SSB)

1. **Kafka data source** **`local`** (brokers, SASL/SSL, PLAIN, default truststore) — **[flink-kafka-data-source.md](./flink-kafka-data-source.md)**  
2. **Kafka table** **`space_invaders_events`** on cluster **`local`**, topic **`space_invaders_events`**, format **JSON**  
3. **Materialized view** **`top10player`** — **[flink-top10player-mv.sql](./flink-top10player-mv.sql)**

## Create the materialized view (Flink SQL)

Full script: **[flink-top10player-mv.sql](./flink-top10player-mv.sql)** (Kafka table DDL + `top10player` MV).

The MV uses a **HOP (sliding) window**: **1 hour** size, **1 minute** slide on **`eventTimestamp`**, max **`newScore`** from **`data_json`** on `score.changed` events.

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS top10player
COMMENT 'Top 10 by max score — 1h sliding window, 1m slide'
AS
SELECT
  CAST(window_start AS STRING)  AS window_start,
  CAST(window_end AS STRING)    AS window_end,
  CAST(player_rank AS INT)      AS player_rank,
  player_id,
  player_name,
  score
FROM (
  SELECT
    window_start,
    window_end,
    player_id,
    player_name,
    score,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end
      ORDER BY score DESC, player_id
    ) AS player_rank
  FROM (
    SELECT
      window_start,
      window_end,
      player_id,
      MAX(player_name) AS player_name,
      MAX(CAST(JSON_VALUE(data_json, '$.newScore') AS INT)) AS score
    FROM TABLE(
      HOP(
        TABLE `ssb`.`ssb_default`.`space_invaders_events`,
        DESCRIPTOR(`eventTimestamp`),
        INTERVAL '1' MINUTE,
        INTERVAL '1' HOUR
      )
    )
    WHERE event_type = 'score.changed'
      AND JSON_VALUE(data_json, '$.newScore') IS NOT NULL
    GROUP BY window_start, window_end, player_id
  )
)
WHERE player_rank <= 10;
```

NiFi **PutIcebergRecord** namespace must be **`space_invaders`** if you also land events in Iceberg. For the leaderboard MV, ensure producers write **`score.changed`** records to Kafka topic **`space_invaders_events`** with **`data_json`** containing **`newScore`**. Wire the SSB MVE query id into [serve-with-nifi-proxy.py](./serve-with-nifi-proxy.py) / [top10-leaderboard.html](./top10-leaderboard.html) if your gateway URL differs.

## Run locally (recommended)

The browser cannot call the gateway with CDP Basic auth directly (CORS). Use the dev server proxy and **environment variables** for credentials — do not commit passwords.

```bash
cd /path/to/space-invaders-arcade   # git clone root — see sync-local-mac.md

export SSB_USER='your-cdp-user'
export SSB_PASSWORD='your-cdp-password'
python3 serve-with-nifi-proxy.py
```

See [sync-local-mac.md](./sync-local-mac.md) for clone and Mac setup.

Open (any of these):

```text
http://127.0.0.1:8080/top10-leaderboard.html
http://127.0.0.1:8080/leaderboard
```

If you get **404**, confirm `top10-leaderboard.html` is in the repo root next to `serve-with-nifi-proxy.py`, then restart the server.

The page calls same-origin `GET /api/ssb/top10?limit=100` (proxied with Basic auth). Alias: `/api/ssb/top10player`.

**proj-si MVE example:** `https://proj-si-gateway.se-sandb.a465-9q4k.cloudera.site/proj-si/cdp-proxy-api/ssb-mve-api/api/v1/query/5195/top10?key=…&limit=100` — see [cdp-full-setup-proj-si.md](./cdp-full-setup-proj-si.md).

## Direct mode (no proxy)

Open the HTML with `?proxy=0` and use **CDP LOGIN** — username/password stored in `sessionStorage` only. Works only if the gateway sends CORS headers allowing your origin (often it does not).

## Query params

| Param | Default | Meaning |
|-------|---------|---------|
| `limit` | `100` | Passed to SSB API |
| `proxy` | `1` | `0` = direct gateway URL |
| `api` | built-in gateway URL | Override MVE endpoint |

Auto-refresh every 20 seconds; dropdown selects historical 1-minute windows.
