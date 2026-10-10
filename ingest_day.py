"""
ingest_day.py — Phase 2, Step 2: land one day of GH Archive into the S3 raw zone.

For each of the 24 hours in a day:
  - skip it if it's already in S3 (safe to rerun)
  - otherwise stream it from GH Archive straight into S3 (nothing saved locally)
  - if GH Archive doesn't have that hour, log it as missing and keep going

Files are stored exactly as downloaded (still gzipped NDJSON), Hive-style:
  s3://<bucket>/raw/gharchive/dt=YYYY-MM-DD/hour=HH/YYYY-MM-DD-H.json.gz

Usage:
    python ingest_day.py 2026-09-27 --bucket your-bucket-name

Requires: boto3, and AWS credentials set up with `aws configure`
(credentials are never stored in this file).
"""

import argparse
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

SOURCE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"
USER_AGENT = "Mozilla/5.0 (gh-archive-learning-project)"
RAW_PREFIX = "raw/gharchive"
MAX_ATTEMPTS = 3          # tries per hour for temporary errors
TIMEOUT_SECONDS = 300


def s3_key(date: str, hour: int) -> str:
    """Where one hour lives in S3.

    Partition folder uses a 2-digit hour (hour=05) so folders sort correctly.
    File name keeps GH Archive's original name (no leading zero) so it matches the source.
    """
    return f"{RAW_PREFIX}/dt={date}/hour={hour:02d}/{date}-{hour}.json.gz"


def already_in_s3(s3, bucket: str, key: str) -> bool:
    """True if the object already exists. Needs s3:ListBucket so a missing key returns 404, not 403."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def stream_hour_to_s3(s3, bucket: str, key: str, url: str) -> int:
    """Download from GH Archive and upload to S3 in one stream. Returns bytes uploaded."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        s3.upload_fileobj(
            response,
            bucket,
            key,
            ExtraArgs={"Metadata": {"source-url": url}},
        )
    # S3 writes are all-or-nothing: if the upload failed, no object exists.
    return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]


def ingest_hour(s3, bucket: str, date: str, hour: int) -> str:
    """Process one hour. Returns 'skipped', 'uploaded', 'missing', or 'failed'."""
    key = s3_key(date, hour)
    url = SOURCE_URL.format(date=date, hour=hour)

    if already_in_s3(s3, bucket, key):
        print(f"  hour {hour:02d}: skipped (already in S3)")
        return "skipped"

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            size = stream_hour_to_s3(s3, bucket, key, url)
            print(f"  hour {hour:02d}: uploaded ({size / 1_000_000:.1f} MB)")
            return "uploaded"
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                # GH Archive storage answers 403/404 when an hour doesn't exist.
                print(f"  hour {hour:02d}: missing at source (HTTP {e.code})")
                return "missing"
            error = f"HTTP {e.code}"
        except Exception as e:  # network hiccup, timeout, S3 error, etc.
            error = f"{type(e).__name__}: {e}"

        if attempt < MAX_ATTEMPTS:
            wait = 5 * attempt
            print(f"  hour {hour:02d}: attempt {attempt} failed ({error}); retrying in {wait}s")
            time.sleep(wait)
        else:
            print(f"  hour {hour:02d}: FAILED after {MAX_ATTEMPTS} attempts ({error})")
    return "failed"


def main():
    parser = argparse.ArgumentParser(description="Land one day of GH Archive into S3.")
    parser.add_argument("date", help="Day to ingest, YYYY-MM-DD (UTC)")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        sys.exit(f"Date must look like 2026-09-27, got: {args.date}")

    s3 = boto3.client("s3")
    print(f"Ingesting {args.date} into s3://{args.bucket}/{RAW_PREFIX}/dt={args.date}/")

    results = {"uploaded": [], "skipped": [], "missing": [], "failed": []}
    for hour in range(24):
        status = ingest_hour(s3, args.bucket, args.date, hour)
        results[status].append(hour)

    print("\nSummary")
    for status, hours in results.items():
        listed = ", ".join(f"{h:02d}" for h in hours) if hours else "-"
        print(f"  {status:<9} {len(hours):>2}   {listed}")

    if results["failed"]:
        print("\nSome hours failed. Rerun the same command; finished hours will be skipped.")
        sys.exit(1)


if __name__ == "__main__":
    main()
