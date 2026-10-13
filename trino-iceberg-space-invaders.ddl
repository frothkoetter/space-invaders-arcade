-- Space Invaders game events — Iceberg table for Trino
-- Companion: nifi-iceberg-ingest-flow.md (NiFi PutIcebergRecord → this table)
-- Avro schema: space-invaders-events.avsc (column names align with this DDL)
--
-- Adjust catalog and schema names to your cluster:
--   YOUR_CATALOG  → Trino iceberg catalog (often "iceberg")
--   YOUR_SCHEMA   → Hive/Iceberg namespace (example: arcade)
--   YOUR_WAREHOUSE → s3a://... or hdfs://... (set on catalog / HMS, not in this DDL)

-- CREATE SCHEMA IF NOT EXISTS iceberg.arcade;

CREATE TABLE IF NOT EXISTS iceberg.arcade.space_invaders_events (
    -- NiFi UpdateRecord / PutIcebergRecord column names (snake_case)
    event_id        VARCHAR,
    event_type      VARCHAR,
    occurred_at     TIMESTAMP(6) WITH TIME ZONE,
    session_id      VARCHAR,
    player_id       VARCHAR,
    player_name     VARCHAR,
    game_version    VARCHAR,
    level           INTEGER,
    frame           BIGINT,
    -- Game "data" object stored as JSON text (or parse with json_parse(data_json) in queries)
    data_json       VARCHAR,
    -- Optional: populated by NiFi UpdateRecord from ${now()} if you add ingest_ts
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET'
    -- Partitioning recommendation (uncomment one strategy after table creation policy review):
    -- , partitioning = ARRAY['day(occurred_at)']
    -- Alternative for skew-heavy workloads:
    -- , partitioning = ARRAY['event_type', 'day(occurred_at)']
);

COMMENT ON TABLE iceberg.arcade.space_invaders_events IS
    'Arcade Space Invaders publishGameEvent envelopes from NiFi ListenHTTP ingest';

COMMENT ON COLUMN iceberg.arcade.space_invaders_events.event_id IS
    'Game JSON: eventId — NiFi source field eventId → event_id';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.event_type IS
    'Game JSON: eventType — e.g. invader.killed, score.changed';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.occurred_at IS
    'Game JSON: occurredAt (ISO-8601)';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.session_id IS
    'Game JSON: sessionId';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.player_id IS
    'Game JSON: playerId';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.player_name IS
    'Game JSON: playerName';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.game_version IS
    'Game JSON: gameVersion (build tag)';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.level IS
    'Game JSON: level';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.frame IS
    'Game JSON: frame';
COMMENT ON COLUMN iceberg.arcade.space_invaders_events.data_json IS
    'Game JSON: data object serialized to string; query with json_parse(data_json)';

-- Example analytics
-- SELECT event_type,
--        count(*) AS events,
--        approx_distinct(player_id) AS players
-- FROM iceberg.arcade.space_invaders_events
-- WHERE occurred_at >= current_date - interval '7' day
-- GROUP BY 1
-- ORDER BY events DESC;

-- Example: extract score from data payload
-- SELECT event_id,
--        cast(json_extract(json_parse(data_json), '$.score') AS integer) AS score
-- FROM iceberg.arcade.space_invaders_events
-- WHERE event_type = 'score.changed';
