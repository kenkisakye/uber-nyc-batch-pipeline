"""Quarterly PySpark release: three validated months -> published demand tables.

For each quarter:
1. Record the run as "running" in pipeline-state.
2. Require _validated.json for all three months of the quarter.
3. Skip if the current release was built from exactly these source files
   (unless --force).
4. Read the processed rows and aggregate them into two tables:
     demand_by_cell_hour  pickups per grid cell, date and hour
     demand_by_base_day   pickups per Uber base and date
5. Reconcile: rows read = valid rows recorded by validation.
6. Write both tables into a NEW release folder. The forecasting app does not
   see it yet, because it only follows the quarter's manifest.
7. Read the tables back; both must total exactly the expected pickups.
8. Write quarter=<q>/_manifest.json LAST. This single write switches the app
   to the new release. If anything fails before it, the manifest still points
   to the previous release, which stays complete and readable.
9. Record "succeeded" or "failed" with counts and timings.

--fail-before-manifest stops the run deliberately between steps 7 and 8, to
demonstrate that a failed release leaves the previous one live.
"""
import argparse
import sys
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import (get_logger, read_json, s3_client, short_error,
                    spark_session, utc_now, write_json, STATE_BUCKET)

QUARTERS = {
    "2014-Q2": ["2014-04", "2014-05", "2014-06"],
    "2014-Q3": ["2014-07", "2014-08", "2014-09"],
}
PUBLISHED = "published"

log = get_logger("release")


class ReleaseError(Exception):
    """A problem that retrying will not fix."""


def aggregate(rows: DataFrame):
    """The two published tables, built from processed (validated) rows."""
    by_cell_hour = (rows.groupBy("pickup_date", "pickup_hour", "day_of_week",
                                 "cell_id", "cell_lat", "cell_lon")
                    .agg(F.count(F.lit(1)).alias("pickups")))
    by_base_day = (rows.groupBy("pickup_date", "day_of_week", "base")
                   .agg(F.count(F.lit(1)).alias("pickups")))
    return {"demand_by_cell_hour": by_cell_hour, "demand_by_base_day": by_base_day}


def release_quarter(spark, s3, quarter, force=False, fail_before_manifest=False):
    months = QUARTERS[quarter]
    manifest_key = f"quarter={quarter}/_manifest.json"
    state_key = f"release/{quarter}.json"

    started = time.monotonic()
    run_id = uuid.uuid4().hex[:12]
    state = {"job": "release", "quarter": quarter, "run_id": run_id,
             "status": "running", "started_at": utc_now()}
    write_json(s3, STATE_BUCKET, state_key, state)
    log.info("quarter=%s run_id=%s started", quarter, run_id)

    try:
        markers = {}
        for month in months:
            year, mon = month.split("-")
            marker = read_json(s3, "processed", f"pickups/year={year}/month={mon}/_validated.json")
            if not marker:
                raise ReleaseError(f"{month} is not validated yet (no _validated.json)")
            markers[month] = marker
        sources = {m: markers[m]["source_sha256"] for m in months}
        expected = sum(markers[m]["rows_valid"] for m in months)

        current = read_json(s3, PUBLISHED, manifest_key)
        if current and current.get("sources") == sources and not force:
            state.update(status="succeeded", outcome="skipped, release is up to date",
                         release_id=current["release_id"], total_pickups=current["total_pickups"],
                         finished_at=utc_now(),
                         seconds={"total": round(time.monotonic() - started, 2)})
            write_json(s3, STATE_BUCKET, state_key, state)
            log.info("quarter=%s skipped, release %s is up to date", quarter, current["release_id"])
            return state

        # ---- read and aggregate
        t0 = time.monotonic()
        paths = [f"s3a://processed/pickups/year={m[:4]}/month={m[5:]}" for m in months]
        rows = spark.read.parquet(*paths).cache()
        rows_read = rows.count()
        if rows_read != expected:
            raise ReleaseError(f"read {rows_read} processed rows, validation recorded {expected}")
        tables = aggregate(rows)
        aggregate_seconds = time.monotonic() - t0

        # ---- write into a new release folder, then check what was written
        t0 = time.monotonic()
        release_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + run_id[:6]
        release_prefix = f"quarter={quarter}/release={release_id}"
        table_info = {}
        for name, df in tables.items():
            path = f"s3a://{PUBLISHED}/{release_prefix}/{name}"
            df.coalesce(1).write.mode("overwrite").parquet(path)
            written = spark.read.parquet(path)
            total = written.agg(F.sum("pickups")).first()[0] or 0
            if total != expected:
                raise ReleaseError(f"{name} totals {total} pickups, expected {expected}")
            table_info[name] = {"path": f"s3://{PUBLISHED}/{release_prefix}/{name}/",
                                "rows": written.count(), "total_pickups": total}
        rows.unpersist()
        write_seconds = time.monotonic() - t0

        if fail_before_manifest:
            raise RuntimeError("simulated failure before the manifest was written (failure test)")

        # ---- publish: the manifest is the last write
        manifest = {
            "quarter": quarter, "release_id": release_id, "created_at": utc_now(),
            "run_id": run_id, "months": months, "sources": sources,
            "total_pickups": expected, "tables": table_info,
            "previous_release_id": current.get("release_id") if current else None,
            "notes": {"grid_degrees": 0.01,
                      "pickup_time": "local New York time as delivered, no time zone"},
        }
        write_json(s3, PUBLISHED, manifest_key, manifest)

        state.update(status="succeeded", outcome="published", release_id=release_id,
                     total_pickups=expected, tables=table_info, finished_at=utc_now(),
                     seconds={"read_and_aggregate": round(aggregate_seconds, 2),
                              "write_and_check": round(write_seconds, 2),
                              "total": round(time.monotonic() - started, 2)})
        write_json(s3, STATE_BUCKET, state_key, state)
        log.info("quarter=%s published release=%s pickups=%d seconds=%.1f",
                 quarter, release_id, expected, state["seconds"]["total"])
        return state

    except Exception as exc:
        state.update(status="failed", error=str(exc)[:2000], error_type=type(exc).__name__,
                     finished_at=utc_now(),
                     seconds={"total": round(time.monotonic() - started, 2)})
        try:
            write_json(s3, STATE_BUCKET, state_key, state)
        except Exception:
            log.exception("could not record the failure in pipeline-state")
        log.error("quarter=%s failed: %s", quarter, short_error(exc))
        raise


def main():
    parser = argparse.ArgumentParser(description="Quarterly PySpark release")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--quarter", choices=sorted(QUARTERS))
    group.add_argument("--all", action="store_true")
    parser.add_argument("--force", action="store_true", help="publish again even if up to date")
    parser.add_argument("--fail-before-manifest", action="store_true",
                        help="failure test: stop after writing the tables, before publishing")
    args = parser.parse_args()

    spark = spark_session("release-quarter")
    s3 = s3_client()
    results = []
    for quarter in (sorted(QUARTERS) if args.all else [args.quarter]):
        try:
            results.append(release_quarter(spark, s3, quarter, args.force,
                                           args.fail_before_manifest))
        except Exception as exc:
            results.append({"quarter": quarter, "status": "failed", "error": short_error(exc)})
    spark.stop()

    print(f"\n{'quarter':<8} {'status':<10} {'pickups':>10} {'seconds':>8}  {'release':<26} outcome")
    for r in results:
        print(f"{r['quarter']:<8} {r['status']:<10} {r.get('total_pickups', 0):>10,} "
              f"{r.get('seconds', {}).get('total', 0):>8}  {r.get('release_id', ''):<26} "
              f"{r.get('outcome', r.get('error', ''))}")
    return 1 if any(r["status"] == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
