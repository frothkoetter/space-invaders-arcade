-- Flink SQL — materialized view top10player (Flink SSB / Streaming SQL)
-- Feeds [top10-leaderboard.html](./top10-leaderboard.html) via SSB MVE API (query name: top10player).
--
-- Primary source: Kafka topic space_invaders_events (virtual table in ssb.ssb_default).
-- Optional batch/archive source: iceberg.space_invaders.space_invaders_events (see trino-iceberg-space-invaders.ddl).
--
-- Output columns (must match leaderboard UI):
--   window_start, window_end, player_rank, player_id, player_name, score
--
-- Step 0 (SSB UI): Register Kafka data source — see flink-kafka-data-source.md
--   Name: local | Protocol: SASL/SSL | SASL: PLAIN | Default TrustStore: ON
--   Brokers: proj-si-master1…:9093, proj-si-master0…:9093

-- ---------------------------------------------------------------------------
-- Step 1 — Virtual table: Kafka topic (SSB UI or SQL)
-- ---------------------------------------------------------------------------
-- UI: Kafka Table → Cluster "local", Topic "space_invaders_events", Format JSON.
-- Or run DDL below (same connector settings as data source "local").
-- Replace YOUR_* placeholders; never commit real passwords.

CREATE TABLE IF NOT EXISTS `ssb`.`ssb_default`.`space_invaders_events` (
  `event_id`       VARCHAR(2147483647),
  `event_type`     VARCHAR(2147483647),
  `occurred_at`    BIGINT,
  `session_id`     VARCHAR(2147483647),
  `player_id`      VARCHAR(2147483647),
  `player_name`    VARCHAR(2147483647),
  `game_version`   VARCHAR(2147483647),
  `level`          BIGINT,
  `frame`          BIGINT,
  `data_json`      VARCHAR(2147483647),
  `ingest_ts`      VARCHAR(2147483647),
  `eventTimestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp',
  WATERMARK FOR `eventTimestamp` AS `eventTimestamp` - INTERVAL '3' SECOND
) WITH (
  'connector'                        = 'kafka',
  'topic'                            = 'space_invaders_events',
  'format'                           = 'json',
  'scan.startup.mode'                = 'earliest-offset',
  'properties.bootstrap.servers'     = 'proj-si-master1.se-sandb.a465-9q4k.cloudera.site:9093,proj-si-master0.se-sandb.a465-9q4k.cloudera.site:9093',
  'properties.security.protocol'     = 'SASL_SSL',
  'properties.sasl.mechanism'        = 'PLAIN',
  'properties.sasl.jaas.config'      = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_CDP_USER" password="YOUR_CDP_PASSWORD";',
  'properties.ssl.truststore.location' = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks',
  'properties.ssl.truststore.password' = 'YOUR_TRUSTSTORE_PASSWORD',
  'properties.auto.offset.reset'     = 'earliest',
  'properties.request.timeout.ms'    = '120000',
  'properties.transaction.timeout.ms'  = '900000'
);

-- Game event time: occurred_at is epoch milliseconds (see index.html publishGameEvent).
-- MV below uses eventTimestamp (Kafka metadata) for windows; alternatively use:
--   TO_TIMESTAMP_LTZ(occurred_at, 3) AS event_time

-- ---------------------------------------------------------------------------
-- Step 2 — Materialized view: top 10 pilots (moving / hopping window)
-- ---------------------------------------------------------------------------
-- Window semantics:
--   • SIZE  1 HOUR  — each window includes all score events in the trailing hour
--   • SLIDE 1 MINUTE — window advances every minute (leaderboard refresh cadence)
-- Flink HOP = sliding window with 1h lookback, updated every 1m.
-- Score: score.changed events; max newScore per player within the window.
--
-- If replacing an older tumble MV, drop first: DROP MATERIALIZED VIEW IF EXISTS top10player;

CREATE MATERIALIZED VIEW IF NOT EXISTS top10player
COMMENT 'Top 10 by max score — 1h sliding window, 1m slide (Kafka event time)'
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

-- ---------------------------------------------------------------------------
-- Optional — strict 1-hour event retention on source (late-data cap)
-- ---------------------------------------------------------------------------
-- Uncomment if you want the MV to ignore game events older than 1h by occurred_at:
--
-- CREATE TEMPORARY VIEW space_invaders_events_last_hour AS
-- SELECT *
-- FROM `ssb`.`ssb_default`.`space_invaders_events`
-- WHERE TO_TIMESTAMP_LTZ(occurred_at, 3) >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
--
-- Then use TABLE space_invaders_events_last_hour inside HOP(...) above.

-- ---------------------------------------------------------------------------
-- Step 3 — Deploy in CDP (typical)
-- ---------------------------------------------------------------------------
-- 0. Data Sources → Kafka data source "local" (flink-kafka-data-source.md)
-- 1. Kafka table space_invaders_events (Step 1 UI or SQL)
-- 2. CREATE MATERIALIZED VIEW top10player (Step 2)
-- 3. In SSB UI, add Materialized View endpoint named top10 (API path /query/{id}/top10)
-- 4. Wire MVE URL into top10-leaderboard.html / serve-with-nifi-proxy.py (SSB_UPSTREAM)
--
-- Sanity (latest sliding windows — up to ~60 overlapping 1h windows in the last hour):
-- SELECT * FROM top10player ORDER BY window_end DESC, player_rank LIMIT 50;
--
-- SSB / Flink state: ensure state TTL >= 1h if your cluster requires explicit retention.

-- ---------------------------------------------------------------------------
-- Optional — Iceberg source (NiFi PutIceberg path, not Kafka)
-- ---------------------------------------------------------------------------
-- CREATE TABLE iceberg_space_invaders_events ( ... ) WITH ('connector' = 'iceberg', ...);
-- Use TUMBLE(..., DESCRIPTOR(occurred_at), ...) when occurred_at is TIMESTAMP(3).
