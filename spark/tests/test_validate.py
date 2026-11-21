"""Unit tests for the quarterly aggregation, on a handful of processed rows.

Runs inside the Spark container, without any storage:
    docker compose run --rm spark test
"""

import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "jobs"))

from pyspark.sql import SparkSession  # noqa: E402
from release_quarter import aggregate  # noqa: E402

spark = (
    SparkSession.builder.master("local[1]")
    .appName("tests")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

COLUMNS = [
    "pickup_ts",
    "pickup_date",
    "pickup_hour",
    "day_of_week",
    "cell_lat",
    "cell_lon",
    "cell_id",
    "base",
]
ROWS = [
    (
        datetime(2014, 4, 1, 8, 5),
        date(2014, 4, 1),
        8,
        3,
        Decimal("40.76"),
        Decimal("-73.96"),
        "40.76_-73.96",
        "B02512",
    ),
    (
        datetime(2014, 4, 1, 8, 40),
        date(2014, 4, 1),
        8,
        3,
        Decimal("40.76"),
        Decimal("-73.96"),
        "40.76_-73.96",
        "B02764",
    ),
    (
        datetime(2014, 4, 1, 9, 1),
        date(2014, 4, 1),
        9,
        3,
        Decimal("40.76"),
        Decimal("-73.96"),
        "40.76_-73.96",
        "B02512",
    ),
    (
        datetime(2014, 4, 2, 8, 0),
        date(2014, 4, 2),
        8,
        4,
        Decimal("40.64"),
        Decimal("-73.79"),
        "40.64_-73.79",
        "B02512",
    ),
]
rows = spark.createDataFrame(ROWS, COLUMNS)
tables = aggregate(rows)
failures = 0


def check(name, cond):
    global failures
    print(("PASS " if cond else "FAIL ") + name)
    failures += not cond


cell = {
    (r["cell_id"], str(r["pickup_date"]), r["pickup_hour"]): r["pickups"]
    for r in tables["demand_by_cell_hour"].collect()
}
check(
    "two pickups in the same cell and hour are counted together",
    cell[("40.76_-73.96", "2014-04-01", 8)] == 2,
)
check(
    "a different hour is a separate row", cell[("40.76_-73.96", "2014-04-01", 9)] == 1
)
check(
    "a different cell is a separate row", cell[("40.64_-73.79", "2014-04-02", 8)] == 1
)

base = {
    (r["base"], str(r["pickup_date"])): r["pickups"]
    for r in tables["demand_by_base_day"].collect()
}
check(
    "pickups per base and day",
    base[("B02512", "2014-04-01")] == 2 and base[("B02764", "2014-04-01")] == 1,
)

for name, df in tables.items():
    total = df.agg({"pickups": "sum"}).first()[0]
    check(f"{name} totals all {len(ROWS)} pickups", total == len(ROWS))
check(
    "published tables hold no exact time or coordinates",
    all("pickup_ts" not in df.columns for df in tables.values()),
)

spark.stop()
print("\nall release tests passed" if failures == 0 else f"\n{failures} test(s) failed")
sys.exit(1 if failures else 0)
