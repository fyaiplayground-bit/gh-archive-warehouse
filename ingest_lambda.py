"""
ingest_lambda.py — Phase 2, Step 6.3: AWS Lambda entry point that lands ONE hour
of GH Archive in the S3 raw zone.

It only wraps ingest_hour() from ingest_day.py, so the laptop CLI and Lambda share
exactly the same download logic (skip if already in S3, stream to S3, retry
temporary errors).

Event:    {"date": "2026-09-28", "hour": 0}
Env var:  BUCKET = fl-gharchive-raw-2026
Returns:  {"date", "hour", "status", "key"}   status = uploaded | skipped | missing

Status handling:
  uploaded / skipped -> success
  missing            -> success, logged as WARNING (GH Archive has real gaps; alert in 6.8)
  failed             -> raise, so the invocation fails and Airflow can retry / alert

Deploy: zip ingest_day.py + ingest_lambda.py together; handler = ingest_lambda.handler
"""

import json
import os
from datetime import datetime

import boto3

from ingest_day import ingest_hour, s3_key

BUCKET = os.environ["BUCKET"]   # fail at start-up if the config is missing
s3 = boto3.client("s3")         # created once per container, reused across invocations


class IngestError(Exception):
    """Raised when an hour could not be ingested after all retries."""


def parse_event(event: dict) -> tuple[str, int]:
    """Validate the input before touching the network."""
    date = event.get("date")
    hour = event.get("hour")
    try:
        datetime.strptime(str(date), "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"'date' must look like 2026-09-28, got: {date!r}")
    try:
        hour = int(hour)
    except (TypeError, ValueError):
        raise ValueError(f"'hour' must be a number 0-23, got: {hour!r}")
    if not 0 <= hour <= 23:
        raise ValueError(f"'hour' must be 0-23, got: {hour}")
    return date, hour


def handler(event, context):
    date, hour = parse_event(event)
    key = s3_key(date, hour)
    print(f"Ingesting {date} hour {hour:02d} -> s3://{BUCKET}/{key}")

    status = ingest_hour(s3, BUCKET, date, hour)
    result = {"date": date, "hour": hour, "status": status, "key": key}

    if status == "failed":
        raise IngestError(f"{date} hour {hour:02d} failed after retries")
    if status == "missing":
        print(f"WARNING: {date} hour {hour:02d} is missing at GH Archive")

    print("RESULT " + json.dumps(result))
    return result