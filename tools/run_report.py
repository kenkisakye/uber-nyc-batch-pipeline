"""Run-time report: reads the run records in the pipeline-state bucket and
prints one table with the status, rows and duration of every stage.

Run on your own machine, from the repository root (plain Python, no packages):
    python3 tools/run_report.py > docs/evidence/run-report.md

Reads through the filer web interface on localhost:8888, which is reachable
from this machine only.
"""
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime

FILER = "http://localhost:8888/buckets/pipeline-state"
MONTHS = ["2014-04", "2014-05", "2014-06", "2014-07", "2014-08", "2014-09"]
QUARTERS = ["2014-Q2", "2014-Q3"]
STAGES = [("ingestion", MONTHS), ("validation", MONTHS), ("release", QUARTERS)]


def fetch(stage, unit):
    try:
        with urllib.request.urlopen(f"{FILER}/{stage}/{unit}.json", timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def parse_time(value):
    return datetime.fromisoformat(value) if value else None


records = []
for stage, units in STAGES:
    for unit in units:
        rec = fetch(stage, unit)
        records.append((stage, unit, rec))

print("# Pipeline run report\n")
print(f"Generated {datetime.now().astimezone().isoformat(timespec='seconds')} "
      "from the run records in `pipeline-state`.\n")
print("| Stage | Unit | Status | Outcome | Rows in | Valid / pickups | Rejected | Seconds | Breakdown (s) |")
print("|---|---|---|---|---:|---:|---:|---:|---|")

stage_seconds = {}
starts, ends = [], []
for stage, unit, rec in records:
    if rec is None:
        print(f"| {stage} | {unit} | missing | | | | | | |")
        continue
    secs = rec.get("seconds", {})
    total = secs.get("total", 0)
    stage_seconds[stage] = stage_seconds.get(stage, 0) + total
    breakdown = ", ".join(f"{k} {v}" for k, v in secs.items() if k != "total")
    rows_in = rec.get("rows_in", rec.get("rows", ""))
    out = rec.get("rows_valid", rec.get("total_pickups", ""))
    rejected = rec.get("rows_rejected", "")
    fmt = lambda v: f"{v:,}" if isinstance(v, int) else v  # noqa: E731
    print(f"| {stage} | {unit} | {rec['status']} | {rec.get('outcome', rec.get('error', ''))[:60]} | "
          f"{fmt(rows_in)} | {fmt(out)} | {fmt(rejected)} | {total} | {breakdown} |")
    if rec.get("started_at"):
        starts.append(parse_time(rec["started_at"]))
    if rec.get("finished_at"):
        ends.append(parse_time(rec["finished_at"]))

print("\n| Stage | Total job seconds |")
print("|---|---:|")
for stage, _ in STAGES:
    print(f"| {stage} | {stage_seconds.get(stage, 0):.1f} |")

if starts and ends:
    wall = (max(ends) - min(starts)).total_seconds()
    print(f"\nFirst job started {min(starts).isoformat()}, last job finished "
          f"{max(ends).isoformat()}: **{wall / 60:.1f} minutes** end to end, including "
          "Spark start-up for every job and the scheduler's pauses between jobs.")

failed = [f"{s} {u}" for s, u, r in records if r is None or r["status"] != "succeeded"]
print("\nAll stages succeeded." if not failed else f"\nNot succeeded: {', '.join(failed)}")
sys.exit(1 if failed else 0)
