-- Space Invaders game events — Hive DDL (Iceberg table)
-- Aligns with nifi-jsontree-schema-text.json / space-invaders-events.avsc (snake_case)
-- NiFi PutIcebergRecord: set Namespace/Database + Table Name to match below

-- USE arcade;
-- CREATE DATABASE IF NOT EXISTS arcade;

CREATE TABLE IF NOT EXISTS arcade.space_invaders_events (
    event_id        STRING COMMENT 'Game eventId (UUID)',
    event_type      STRING COMMENT 'Game eventType e.g. invader.killed',
    occurred_at     TIMESTAMP COMMENT 'Game occurredAt (ISO-8601)',
    session_id      STRING,
    player_id       STRING,
    player_name     STRING,
    game_version    STRING,
    level           INT,
    frame           BIGINT,
    data_json       STRING COMMENT 'JSON string of game data object',
    ingest_ts       TIMESTAMP COMMENT 'NiFi ingest time (optional)'
)
STORED BY ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

-- Default database variant (matches CREATE without DB prefix):
-- CREATE TABLE IF NOT EXISTS space_invaders_events ( ... ) STORED BY ICEBERG;
