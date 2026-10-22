-- Athena (workgroup: gharchive) — register silver Parquet as a table.
-- Run each statement separately in the Athena editor.

CREATE DATABASE IF NOT EXISTS gharchive;

CREATE EXTERNAL TABLE IF NOT EXISTS gharchive.silver_events (
  event_id      string,
  event_type    string,
  actor_id      bigint,
  actor_login   string,
  repo_id       bigint,
  repo_name     string,
  org_id        bigint,
  org_login     string,
  is_public     boolean,
  created_at    timestamp,
  payload_json  string
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://fl-gharchive-raw-2026/silver/events/';

-- Register partitions (rerun after new dates are written; Step 6 → partition projection)
MSCK REPAIR TABLE gharchive.silver_events;

-- Check
SHOW PARTITIONS gharchive.silver_events;
