"""Ingestion service: loads monthly Uber CSV files into the raw bucket.

For each month:
1. Record the run as "running" in the pipeline-state bucket.
2. Check the file: expected header, at least one row, and how many
   timestamps fall outside the month or cannot be read. The file itself is
   never changed; rejecting rows is the job of the PySpark validation.
3. Compute the SHA-256 checksum.
4. If the same file is already ingested, skip it. If a different file is
   already ingested for that month, stop: raw files are never overwritten.
5. Upload the file, verify its size, then write _ingest.json next to it.
   The marker is written last, so later jobs only ever see complete files.
6. Record the run as "succeeded" or "failed", with row counts and timings.

Usage:
    ingest.py --month 2014-04
    ingest.py --all
"""
import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

MONTH_FILES = {
    "2014-04": "uber-raw-data-apr14.csv",
    "2014-05": "uber-raw-data-may14.csv",
    "2014-06": "uber-raw-data-jun14.csv",
    "2014-07": "uber-raw-data-jul14.csv",
    "2014-08": "uber-raw-data-aug14.csv",
    "2014-09": "uber-raw-data-sep14.csv",
}
EXPECTED_HEADER = ["Date/Time", "Lat", "Lon", "Base"]
RAW_BUCKET = "raw"
STATE_BUCKET = "pipeline-state"
NOT_FOUND = {"404", "NoSuchKey", "NotFound"}

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s ingestion %(message)s",
)
log = logging.getLogger("ingestion")


class IngestionError(Exception):
    """A problem with the input that retrying will not fix."""


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        # Transient network or storage errors are retried with back-off
        config=Config(s3={"addressing_style": "path"},
                      retries={"max_attempts": 5, "mode": "standard"}),
    )


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_timestamp(value):
    """'4/1/2014 0:11:00' -> (2014, 4, 1, 0, 11, 0), or None if unreadable."""
    try:
        date_part, time_part = value.strip().split(" ")
        month, day, year = (int(x) for x in date_part.split("/"))
        hour, minute, second = (int(x) for x in time_part.split(":"))
        return (year, month, day, hour, minute, second)
    except ValueError:
        return None


def profile_file(path, year, month):
    """Checks the file and returns its checksum, row counts and time range."""
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)

    rows = outside = unreadable = 0
    first = last = None
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header != EXPECTED_HEADER:
            raise IngestionError(f"unexpected header {header}, expected {EXPECTED_HEADER}")
        for row in reader:
            rows += 1
            ts = parse_timestamp(row[0]) if row else None
            if ts is None:
                unreadable += 1
                continue
            if (ts[0], ts[1]) != (year, month):
                outside += 1
            first = ts if first is None or ts < first else first
            last = ts if last is None or ts > last else last

    if rows == 0:
        raise IngestionError("file has no data rows")

    def fmt(t):
        return None if t is None else "%04d-%02d-%02dT%02d:%02d:%02d" % t

    return {
        "sha256": sha.hexdigest(),
        "size_bytes": path.stat().st_size,
        "rows": rows,
        "rows_outside_month": outside,
        "rows_unreadable_timestamp": unreadable,
        "first_pickup": fmt(first),
        "last_pickup": fmt(last),
    }


def read_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in NOT_FOUND:
            return None
        raise


def write_json(s3, bucket, key, data):
    s3.put_object(Bucket=bucket, Key=key, ContentType="application/json",
                  Body=json.dumps(data, indent=2).encode())


def ingest_month(s3, month, data_dir):
    year, mon = (int(x) for x in month.split("-"))
    filename = MONTH_FILES[month]
    prefix = f"year={year}/month={mon:02d}"
    data_key = f"{prefix}/{filename}"
    marker_key = f"{prefix}/_ingest.json"
    state_key = f"ingestion/{month}.json"

    started = time.monotonic()
    state = {"job": "ingestion", "month": month, "run_id": uuid.uuid4().hex[:12],
             "status": "running", "started_at": utc_now()}
    write_json(s3, STATE_BUCKET, state_key, state)
    log.info("month=%s run_id=%s started", month, state["run_id"])

    try:
        path = Path(data_dir) / filename
        if not path.is_file():
            raise IngestionError(f"{path} not found")

        t0 = time.monotonic()
        profile = profile_file(path, year, mon)
        check_seconds = time.monotonic() - t0

        t0 = time.monotonic()
        existing = read_json(s3, RAW_BUCKET, marker_key)
        if existing and existing.get("sha256") == profile["sha256"]:
            outcome = "skipped, identical file already ingested"
        elif existing:
            raise IngestionError(
                "a different file is already ingested for this month "
                f"(sha256 {existing.get('sha256')}); raw files are never overwritten")
        else:
            # No marker: nothing ingested yet, or an earlier run stopped before
            # finishing. Either way it is safe to (re)upload the file.
            s3.upload_file(str(path), RAW_BUCKET, data_key, ExtraArgs={
                "ContentType": "text/csv", "Metadata": {"sha256": profile["sha256"]}})
            stored = s3.head_object(Bucket=RAW_BUCKET, Key=data_key)["ContentLength"]
            if stored != profile["size_bytes"]:
                raise RuntimeError(f"stored size {stored} != local size {profile['size_bytes']}")
            write_json(s3, RAW_BUCKET, marker_key, {
                "source_file": filename, "month": month, **profile,
                "ingested_at": utc_now(), "run_id": state["run_id"]})
            outcome = "ingested"
        upload_seconds = time.monotonic() - t0

        if profile["rows_outside_month"] or profile["rows_unreadable_timestamp"]:
            log.warning("month=%s rows_outside_month=%d rows_unreadable_timestamp=%d "
                        "(left for PySpark validation to quarantine)", month,
                        profile["rows_outside_month"], profile["rows_unreadable_timestamp"])

        state.update(status="succeeded", outcome=outcome, finished_at=utc_now(),
                     raw_object=f"s3://{RAW_BUCKET}/{data_key}", **profile,
                     seconds={"check": round(check_seconds, 2),
                              "upload": round(upload_seconds, 2),
                              "total": round(time.monotonic() - started, 2)})
        write_json(s3, STATE_BUCKET, state_key, state)
        log.info("month=%s %s rows=%d seconds=%.1f", month, outcome,
                 profile["rows"], state["seconds"]["total"])
        return state

    except Exception as exc:
        state.update(status="failed", error=str(exc), error_type=type(exc).__name__,
                     finished_at=utc_now(),
                     seconds={"total": round(time.monotonic() - started, 2)})
        try:
            write_json(s3, STATE_BUCKET, state_key, state)
        except Exception:
            log.exception("could not record the failure in pipeline-state")
        log.error("month=%s failed: %s", month, exc)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", choices=sorted(MONTH_FILES))
    group.add_argument("--all", action="store_true", help="ingest all six months")
    args = parser.parse_args()

    months = sorted(MONTH_FILES) if args.all else [args.month]
    data_dir = os.environ.get("DATA_DIR", "/data")
    s3 = s3_client()

    results = []
    for month in months:
        try:
            results.append(ingest_month(s3, month, data_dir))
        except Exception as exc:
            results.append({"month": month, "status": "failed", "error": str(exc)})

    print(f"\n{'month':<8} {'status':<10} {'rows':>10} {'seconds':>8}  outcome")
    total = 0
    for r in results:
        total += r.get("rows", 0)
        print(f"{r['month']:<8} {r['status']:<10} {r.get('rows', 0):>10,} "
              f"{r.get('seconds', {}).get('total', 0):>8}  {r.get('outcome', r.get('error', ''))}")
    print(f"{'total':<8} {'':<10} {total:>10,}")

    return 1 if any(r["status"] == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
