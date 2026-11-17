-- Athena (workgroup: gharchive) — register silver Parquet as a table.
-- Run each statement separately in the Athena editor.
--
-- Partitions: partition projection (Step 6.2). Athena computes the dt=... folders
-- from the rule in TBLPROPERTIES, so new dates never need to be registered
-- (no MSCK REPAIR / ADD PARTITION). Widen projection.dt.range in Step 7 (2014 data).

CREATE DATABASE IF NOT EXISTS gharchive;

-- Fresh setup: creates the table with projection already on.
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
LOCATION 's3://fl-gharchive-raw-2026/silver/events/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.range'         = '2026-09-01,NOW',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template'   = 's3://fl-gharchive-raw-2026/silver/events/dt=${dt}/'
);

-- Existing table (created in Step 3 without projection): turn projection on in place.
-- Ran once on 2026-11-16. Safe to rerun (it just sets the same properties again).
ALTER TABLE gharchive.silver_events SET TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.range'         = '2026-09-01,NOW',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template'   = 's3://fl-gharchive-raw-2026/silver/events/dt=${dt}/'
);

-- Checks
-- 1. The projection properties are on the table
SHOW TBLPROPERTIES gharchive.silver_events;

-- 2. Existing data still readable: expect 2,955,892
SELECT count(*) FROM gharchive.silver_events WHERE dt = DATE '2026-09-27';

-- 3. A date with no folder: expect 0 rows, 0 bytes scanned
SELECT count(*) FROM gharchive.silver_events WHERE dt = DATE '2026-09-20';

-- 4. Whole projected range: only dates that have files show up
SELECT dt, count(*) AS events FROM gharchive.silver_events GROUP BY dt ORDER BY dt;