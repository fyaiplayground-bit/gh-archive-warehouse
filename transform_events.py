"""
transform_events.py — Phase 2, Step 3: raw (bronze) → silver for one day.

Reads one day of raw GH Archive files, pulls the "label" fields into typed
columns, keeps the payload as JSON text, checks data quality, removes
duplicate events, and writes partitioned Parquet — replacing only that day.

Plain PySpark: the same file runs on a laptop and as an AWS Glue job.
Only the paths change.

Local:
    python transform_events.py --date 2026-09-27 \
        --raw-base data/raw/gharchive \
        --silver-base output/silver/events \
        --driver-memory 4g

Glue (job parameters):
    --date         2026-09-27
    --raw-base     s3://fl-gharchive-raw-2026/raw/gharchive
    --silver-base  s3://fl-gharchive-raw-2026/silver/events

Input layout (same locally and in S3):
    {raw-base}/dt=YYYY-MM-DD/hour=HH/YYYY-MM-DD-H.json.gz
Output layout:
    {silver-base}/dt=YYYY-MM-DD/part-*.parquet
"""

import argparse
import json
import math
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# The label fields we keep (decided in Step 3 exploration).
# Payload stays a JSON string — how to unpack it is a Step 4 decision.
LABEL_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("actor", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
    ])),
    StructField("repo", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
    ])),
    StructField("org", StructType([
        StructField("id", LongType()),
        StructField("login", StringType()),
    ])),
    StructField("public", BooleanType()),
    StructField("created_at", StringType()),
    StructField("payload", StringType()),
])

DEFAULT_ROWS_PER_FILE = 1_000_000  # measured ~166 bytes/row → ~166 MB per file


class DataQualityError(Exception):
    """Raised when the data fails a check. The job stops before writing anything."""


def log(message: str) -> None:
    """Timestamped line on stdout — shows in the terminal locally, in CloudWatch on Glue."""
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[transform_events {now} UTC] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GH Archive raw → silver for one day.")
    parser.add_argument("--date", required=True, help="Day to process, YYYY-MM-DD (UTC)")
    parser.add_argument("--raw-base", required=True, help="Folder containing dt=.../hour=.../ raw files")
    parser.add_argument("--silver-base", required=True, help="Output folder for silver Parquet")
    parser.add_argument("--rows-per-file", type=int, default=DEFAULT_ROWS_PER_FILE,
                        help="Target rows per output file (default 1,000,000)")
    parser.add_argument("--driver-memory", default=None,
                        help="Local runs only, e.g. 4g. Glue sets its own memory.")
    # parse_known_args: Glue adds its own arguments (--JOB_NAME, --TempDir, ...).
    # We read ours and ignore the rest.
    args, _ignored = parser.parse_known_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--date must look like 2026-09-27, got: {args.date}")
    if args.rows_per_file < 1:
        parser.error("--rows-per-file must be at least 1")
    return args


def build_spark(driver_memory: str | None) -> SparkSession:
    builder = SparkSession.builder.appName("gharchive-transform-events")
    if driver_memory:
        # Only takes effect when this script starts Spark itself (local runs).
        builder = builder.master("local[*]").config("spark.driver.memory", driver_memory)
    spark = builder.getOrCreate()

    # Runtime settings — these work locally and on Glue.
    spark.conf.set("spark.sql.session.timeZone", "UTC")                      # dates follow UTC, not the laptop
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")    # replace only the dates we write
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # timestamps get min/max stats
    return spark


def read_raw(spark: SparkSession, raw_base: str, date: str) -> DataFrame:
    """Read every hourly file for one day, with our explicit schema (no inference scan)."""
    path = f"{raw_base.rstrip('/')}/dt={date}/hour=*/*.json.gz"
    log(f"Reading {path}")
    return spark.read.schema(LABEL_SCHEMA).json(path)


def to_silver(raw: DataFrame) -> DataFrame:
    """Flatten and rename the label fields, type the timestamp, add the dt partition column."""
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("org.id").alias("org_id"),
        F.col("org.login").alias("org_login"),
        F.col("public").alias("is_public"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("payload").alias("payload_json"),
    ).withColumn("dt", F.to_date("created_at"))


def profile(silver: DataFrame, date: str) -> dict:
    """All pre-write counts in ONE pass over the data (one action, not four)."""
    row = silver.agg(
        F.count(F.lit(1)).alias("rows_read"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("null_event_ids"),
        F.sum(F.col("created_at").isNull().cast("int")).alias("bad_timestamps"),
        F.sum((F.col("dt").isNotNull() & (F.col("dt") != F.lit(date))).cast("int")).alias("rows_other_dates"),
    ).collect()[0]
    # sum() over zero rows returns None, so default to 0
    return {key: int(row[key] or 0) for key in row.asDict()}


def check_quality(stats: dict) -> None:
    """Fail fast: better no output than wrong output."""
    problems = []
    if stats["rows_read"] == 0:
        problems.append("read 0 rows (wrong path, or this day hasn't been ingested?)")
    if stats["null_event_ids"] > 0:
        problems.append(f"{stats['null_event_ids']} rows have no event_id")
    if stats["bad_timestamps"] > 0:
        problems.append(f"{stats['bad_timestamps']} rows have a created_at that didn't parse")
    if problems:
        raise DataQualityError("; ".join(problems))


def files_needed(row_count: int, rows_per_file: int) -> int:
    return max(1, math.ceil(row_count / rows_per_file))


def main() -> None:
    started = time.time()
    args = parse_args()
    spark = build_spark(args.driver_memory)
    log(f"Spark {spark.version} | date={args.date}")

    # 1. Read + transform. Cache: the checks and the dedup both reuse it,
    #    so the gzip files are read and parsed only once.
    silver = to_silver(read_raw(spark, args.raw_base, args.date)).cache()

    # 2. Profile + quality checks (stop here if anything is wrong — nothing written yet).
    stats = profile(silver, args.date)
    log(f"Profile: {stats}")
    check_quality(stats)

    # 3. Keep only this job's own date. With dynamic overwrite, a stray row from
    #    another day would otherwise REPLACE that day's whole partition.
    on_date = silver.filter(F.col("dt") == F.lit(args.date))
    rows_on_date = stats["rows_read"] - stats["rows_other_dates"]

    # 4. Remove duplicate events — and measure how many there were.
    deduped = on_date.dropDuplicates(["event_id"]).cache()
    rows_out = deduped.count()
    duplicates_removed = rows_on_date - rows_out

    # 5. Choose the file count from the data size, then write.
    #    coalesce (no extra shuffle) — the dedup already spread the data evenly.
    num_files = files_needed(rows_out, args.rows_per_file)
    log(f"Writing {rows_out:,} rows as {num_files} file(s) to {args.silver_base}/dt={args.date}/")
    (deduped
        .coalesce(num_files)
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(args.silver_base))

    deduped.unpersist()
    silver.unpersist()

    summary = {
        "date": args.date,
        "rows_read": stats["rows_read"],
        "rows_other_dates_dropped": stats["rows_other_dates"],
        "duplicates_removed": duplicates_removed,
        "rows_written": rows_out,
        "files_written": num_files,
        "seconds": round(time.time() - started, 1),
    }
    log("SUMMARY " + json.dumps(summary))


if __name__ == "__main__":
    main()
