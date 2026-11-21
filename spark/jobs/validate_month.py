"""Monthly PySpark validation: raw CSV -> processed Parquet + quarantine.

For each month:
1. Record the run as "running" in pipeline-state.
2. Require the ingestion marker raw/.../_ingest.json (only complete files).
3. Skip if this exact source file was already validated (unless --force).
4. Read the CSV with a fixed schema and give every row a reject reason, or
   none if it is valid.
5. Check that Spark read exactly as many rows as ingestion counted.
6. Remove the old marker, then write valid rows to processed and rejected
   rows to quarantine (each month overwrites only its own folder).
7. Check the written row count, then write _validated.json LAST, so the
   quarterly job never reads a half-written month.
8. Record "succeeded" or "failed" with counts and timings.

Data protection: processed rows keep only a grid cell (0.01 degrees, about
1 km), never the exact pickup coordinates. Exact values stay in raw and, for
rejected rows, in quarantine; neither is readable by the forecasting app.
"""

import argparse
import sys
import time
import uuid

from common import (
    STATE_BUCKET,
    delete_if_exists,
    get_logger,
    read_json,
    s3_client,
    short_error,
    spark_session,
    utc_now,
    write_json,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

MONTHS = ["2014-04", "2014-05", "2014-06", "2014-07", "2014-08", "2014-09"]

# The single schema definition for the raw files. Everything is read as text
# and converted explicitly, so bad values become reject reasons, not crashes.
RAW_SCHEMA = StructType(
    [
        StructField("Date/Time", StringType()),
        StructField("Lat", StringType()),
        StructField("Lon", StringType()),
        StructField("Base", StringType()),
    ]
)
TIMESTAMP_FORMAT = "M/d/yyyy H:mm:ss"

# Generous box around New York City, including the airports and nearby New Jersey
LAT_MIN, LAT_MAX = 39.5, 41.5
LON_MIN, LON_MAX = -75.0, -72.0
GRID = 0.01  # degrees

log = get_logger("validation")


class ValidationError(Exception):
    """A problem that retrying will not fix."""


def classify(raw: DataFrame, year: int, month: int) -> DataFrame:
    """Adds typed columns and reject_reason (null when the row is valid).

    The typed columns get new names (lat_num, lon_num, base_code) because
    Spark column names are case-insensitive: "lat" would replace "Lat" and
    the original text needed for the quarantine would be lost.
    """
    ts = F.try_to_timestamp(F.col("Date/Time"), F.lit(TIMESTAMP_FORMAT))
    lat = F.trim(F.col("Lat")).try_cast("double")
    lon = F.trim(F.col("Lon")).try_cast("double")
    base = F.trim(F.col("Base"))
    reason = (
        F.when(ts.isNull(), "unreadable_timestamp")
        .when((F.year(ts) != year) | (F.month(ts) != month), "outside_month")
        .when(lat.isNull() | lon.isNull(), "unreadable_coordinates")
        .when(
            ~lat.between(LAT_MIN, LAT_MAX) | ~lon.between(LON_MIN, LON_MAX),
            "outside_service_area",
        )
        .when(base.isNull() | ~base.rlike(r"^B[0-9]{5}$"), "invalid_base")
    )
    return (
        raw.withColumn("pickup_ts", ts)
        .withColumn("lat_num", lat)
        .withColumn("lon_num", lon)
        .withColumn("base_code", base)
        .withColumn("reject_reason", reason)
    )


def valid_rows(classified: DataFrame) -> DataFrame:
    """Valid rows with the coordinates reduced to a grid cell."""
    cell_lat = (F.floor(F.col("lat_num") / GRID) * GRID).cast("decimal(6,2)")
    cell_lon = (F.floor(F.col("lon_num") / GRID) * GRID).cast("decimal(7,2)")
    return classified.filter(F.col("reject_reason").isNull()).select(
        F.col("pickup_ts"),
        F.to_date("pickup_ts").alias("pickup_date"),
        F.hour("pickup_ts").alias("pickup_hour"),
        F.dayofweek("pickup_ts").alias("day_of_week"),
        cell_lat.alias("cell_lat"),
        cell_lon.alias("cell_lon"),
        F.concat_ws("_", cell_lat.cast("string"), cell_lon.cast("string")).alias(
            "cell_id"
        ),
        F.col("base_code").alias("base"),
    )


def rejected_rows(classified: DataFrame) -> DataFrame:
    """Rejected rows exactly as delivered, with the reason."""
    return classified.filter(F.col("reject_reason").isNotNull()).select(
        F.col("Date/Time").alias("raw_datetime"),
        F.col("Lat").alias("raw_lat"),
        F.col("Lon").alias("raw_lon"),
        F.col("Base").alias("raw_base"),
        F.col("reject_reason"),
    )


def validate_month(spark, s3, month, force=False):
    year, mon = (int(x) for x in month.split("-"))
    raw_prefix = f"year={year}/month={mon:02d}"
    out_prefix = f"pickups/year={year}/month={mon:02d}"
    marker_key = f"{out_prefix}/_validated.json"
    state_key = f"validation/{month}.json"

    started = time.monotonic()
    state = {
        "job": "validation",
        "month": month,
        "run_id": uuid.uuid4().hex[:12],
        "status": "running",
        "started_at": utc_now(),
    }
    write_json(s3, STATE_BUCKET, state_key, state)
    log.info("month=%s run_id=%s started", month, state["run_id"])

    try:
        ingest = read_json(s3, "raw", f"{raw_prefix}/_ingest.json")
        if not ingest:
            raise ValidationError("month not ingested yet (no _ingest.json in raw)")

        existing = read_json(s3, "processed", marker_key)
        if existing and existing.get("source_sha256") == ingest["sha256"] and not force:
            state.update(
                status="succeeded",
                outcome="skipped, already validated",
                finished_at=utc_now(),
                **{
                    k: existing[k]
                    for k in (
                        "rows_in",
                        "rows_valid",
                        "rows_rejected",
                        "rejected_by_reason",
                    )
                },
                seconds={"total": round(time.monotonic() - started, 2)},
            )
            write_json(s3, STATE_BUCKET, state_key, state)
            log.info("month=%s skipped, already validated", month)
            return state

        # ---- read and classify
        t0 = time.monotonic()
        raw = (
            spark.read.option("header", True)
            .schema(RAW_SCHEMA)
            .csv(f"s3a://raw/{raw_prefix}/{ingest['source_file']}")
        )
        classified = classify(raw, year, mon).cache()
        by_reason = {
            r["reject_reason"]: r["count"]
            for r in classified.groupBy("reject_reason").count().collect()
        }
        rows_in = sum(by_reason.values())
        rows_valid = by_reason.pop(None, 0)
        rows_rejected = sum(by_reason.values())
        if rows_in != ingest["rows"]:
            raise ValidationError(
                f"Spark read {rows_in} rows but ingestion counted {ingest['rows']}"
            )
        check_seconds = time.monotonic() - t0

        # ---- write (the old marker goes first, the new one last)
        t0 = time.monotonic()
        delete_if_exists(s3, "processed", marker_key)
        processed_path = f"s3a://processed/{out_prefix}"
        valid_rows(classified).coalesce(2).write.mode("overwrite").parquet(
            processed_path
        )
        (
            rejected_rows(classified)
            .coalesce(1)
            .write.mode("overwrite")
            .parquet(f"s3a://quarantine/{out_prefix}")
        )
        written = spark.read.parquet(processed_path).count()
        if written != rows_valid:
            raise RuntimeError(f"wrote {written} valid rows, expected {rows_valid}")
        classified.unpersist()
        write_seconds = time.monotonic() - t0

        result = {
            "month": month,
            "source_file": ingest["source_file"],
            "source_sha256": ingest["sha256"],
            "rows_in": rows_in,
            "rows_valid": rows_valid,
            "rows_rejected": rows_rejected,
            "rejected_by_reason": by_reason,
            "grid_degrees": GRID,
            "validated_at": utc_now(),
            "run_id": state["run_id"],
        }
        write_json(s3, "processed", marker_key, result)

        state.update(
            status="succeeded",
            outcome="validated",
            finished_at=utc_now(),
            rows_in=rows_in,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rejected_by_reason=by_reason,
            seconds={
                "read_and_check": round(check_seconds, 2),
                "write": round(write_seconds, 2),
                "total": round(time.monotonic() - started, 2),
            },
        )
        write_json(s3, STATE_BUCKET, state_key, state)
        log.info(
            "month=%s validated rows_in=%d valid=%d rejected=%d seconds=%.1f",
            month,
            rows_in,
            rows_valid,
            rows_rejected,
            state["seconds"]["total"],
        )
        return state

    except Exception as exc:
        state.update(
            status="failed",
            error=str(exc)[:2000],
            error_type=type(exc).__name__,
            finished_at=utc_now(),
            seconds={"total": round(time.monotonic() - started, 2)},
        )
        try:
            write_json(s3, STATE_BUCKET, state_key, state)
        except Exception:
            log.exception("could not record the failure in pipeline-state")
        log.error("month=%s failed: %s", month, short_error(exc))
        raise


def main():
    parser = argparse.ArgumentParser(description="Monthly PySpark validation")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", choices=MONTHS)
    group.add_argument("--all", action="store_true")
    parser.add_argument(
        "--force", action="store_true", help="validate again even if already done"
    )
    args = parser.parse_args()

    spark = spark_session("validate-month")
    s3 = s3_client()
    results = []
    for month in MONTHS if args.all else [args.month]:
        try:
            results.append(validate_month(spark, s3, month, args.force))
        except Exception as exc:
            results.append(
                {"month": month, "status": "failed", "error": short_error(exc)}
            )
    spark.stop()

    print(
        f"\n{'month':<8} {'status':<10} {'rows_in':>10} {'valid':>10} {'rejected':>9} "
        f"{'seconds':>8}  outcome"
    )
    totals = [0, 0, 0]
    for r in results:
        vals = [r.get("rows_in", 0), r.get("rows_valid", 0), r.get("rows_rejected", 0)]
        totals = [a + b for a, b in zip(totals, vals)]
        print(
            f"{r['month']:<8} {r['status']:<10} {vals[0]:>10,} {vals[1]:>10,} {vals[2]:>9,} "
            f"{r.get('seconds', {}).get('total', 0):>8}  {r.get('outcome', r.get('error', ''))}"
        )
    print(f"{'total':<8} {'':<10} {totals[0]:>10,} {totals[1]:>10,} {totals[2]:>9,}")
    reasons = {}
    for r in results:
        for k, v in r.get("rejected_by_reason", {}).items():
            reasons[k] = reasons.get(k, 0) + v
    if reasons:
        print(
            "rejected by reason: "
            + ", ".join(f"{k}={v:,}" for k, v in sorted(reasons.items()))
        )

    return 1 if any(r["status"] == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
