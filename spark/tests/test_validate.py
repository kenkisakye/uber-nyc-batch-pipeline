"""Unit tests for the monthly validation rules, on a handful of sample rows.

Runs inside the Spark container, without any storage:
    docker compose run --rm spark test
"""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "jobs"))

from pyspark.sql import SparkSession  # noqa: E402
from validate_month import RAW_SCHEMA, classify, rejected_rows, valid_rows  # noqa: E402

SAMPLE = [
    # (Date/Time, Lat, Lon, Base, expected reject_reason)
    ("4/1/2014 0:11:00", "40.769", "-73.9549", "B02512", None),
    ("4/30/2014 23:59:00", "40.6413", "-73.7781", " B02764 ", None),  # JFK, padded base
    ("5/1/2014 0:01:00", "40.75", "-73.98", "B02512", "outside_month"),
    ("garbage", "40.75", "-73.98", "B02512", "unreadable_timestamp"),
    ("4/2/2014 1:00:00", "abc", "-73.98", "B02512", "unreadable_coordinates"),
    ("4/2/2014 1:00:00", "10.0", "-73.98", "B02512", "outside_service_area"),
    ("4/2/2014 1:00:00", "40.75", "-73.98", "X1", "invalid_base"),
    ("4/2/2014 1:00:00", "40.75", "-73.98", None, "invalid_base"),
]

spark = (
    SparkSession.builder.master("local[1]")
    .appName("tests")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

raw = spark.createDataFrame([s[:4] for s in SAMPLE], RAW_SCHEMA)
classified = classify(raw, 2014, 4)
got = [r["reject_reason"] for r in classified.collect()]
expected = [s[4] for s in SAMPLE]
failures = 0


def check(name, cond):
    global failures
    print(("PASS " if cond else "FAIL ") + name)
    failures += not cond


for row, want, have in zip(SAMPLE, expected, got):
    check(f"{row[0]!r:22} {row[1]!r:9} {row[3]!r:10} -> {want}", want == have)

valid = valid_rows(classified)
check(
    "valid rows hold no exact coordinates",
    not {"lat_num", "lon_num", "Lat", "Lon"} & set(valid.columns),
)
first = valid.orderBy("pickup_ts").first()
check(
    "grid cell 40.769/-73.9549 -> 40.76/-73.96",
    first["cell_lat"] == Decimal("40.76")
    and first["cell_lon"] == Decimal("-73.96")
    and first["cell_id"] == "40.76_-73.96",
)
check(
    "hour and date derived",
    first["pickup_hour"] == 0 and str(first["pickup_date"]) == "2014-04-01",
)
check("padded base trimmed", valid.filter("base = 'B02764'").count() == 1)
check("counts add up", valid.count() + rejected_rows(classified).count() == len(SAMPLE))
rejected = rejected_rows(classified)
check(
    "rejected rows keep the original text",
    dict(rejected.dtypes)["raw_lat"] == "string"
    and rejected.filter("raw_lat = 'abc'").count() == 1
    and rejected.filter("raw_base = 'X1'").count() == 1,
)

spark.stop()
print("\nall tests passed" if failures == 0 else f"\n{failures} test(s) failed")
sys.exit(1 if failures else 0)
