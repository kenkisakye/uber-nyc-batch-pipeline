"""The pipeline's two cycles and the retry logic, used by scheduler.py.

Monthly cycle:   for each month in order, ingest it, then validate it.
Quarterly cycle: release every quarter.

Every job is safe to repeat: work already done for the same source files is
skipped quickly. A cycle can therefore always be run again to catch up, for
example after the machine was switched off.

Failure handling:
- every job call is retried with back-off (RETRY_DELAYS, in seconds)
- a worker that is down, busy or returns an error counts as a failed attempt
- a month that fails ingestion is not validated in that cycle
- the release job itself refuses a quarter whose months are not all validated
- only one cycle runs at a time (file lock shared with manual runs)

Uses only the Python standard library.
"""
import fcntl
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager

MONTHS = ["2014-04", "2014-05", "2014-06", "2014-07", "2014-08", "2014-09"]
QUARTERS = ["2014-Q2", "2014-Q3"]

INGESTION_URL = os.environ.get("INGESTION_URL", "http://ingestion-worker:8000")
SPARK_URL = os.environ.get("SPARK_URL", "http://spark-worker:8000")
STATE_DIR = os.environ.get("STATE_DIR", "/state")
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", "3600"))

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s scheduler %(message)s")
log = logging.getLogger("scheduler")


def retry_delays():
    return [int(x) for x in os.environ.get("RETRY_DELAYS", "30,60,120").split(",") if x.strip()]


def _reason_from_output(lines):
    for line in reversed(lines):
        if "failed" in line or "ERROR" in line:
            return line.strip()[-200:]
    return (lines[-1].strip() if lines else "no output")[-200:]


def call_job(base_url, args, label):
    """Runs one job on a worker, retrying with back-off. Returns True on success."""
    delays = retry_delays()
    attempts = len(delays) + 1
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(
                base_url + "/run", method="POST",
                data=json.dumps({"args": args}).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=JOB_TIMEOUT) as response:
                result = json.loads(response.read())
            if result["exit_code"] == 0:
                log.info("%s succeeded in %.1fs (attempt %d)", label, result["seconds"], attempt)
                return True
            reason = (f"exit code {result['exit_code']}: "
                      f"{_reason_from_output(result.get('output_tail', []))}")
        except urllib.error.HTTPError as e:
            reason = f"worker answered HTTP {e.code}" + (" (busy)" if e.code == 409 else "")
        except (urllib.error.URLError, OSError) as e:
            reason = f"worker unreachable ({getattr(e, 'reason', e)})"

        if attempt < attempts:
            delay = delays[attempt - 1]
            log.warning("%s failed (attempt %d of %d): %s; retrying in %ds",
                        label, attempt, attempts, reason, delay)
            time.sleep(delay)
        else:
            log.error("%s failed after %d attempts: %s", label, attempts, reason)
    return False


@contextmanager
def cycle_lock():
    """Yields True if this process may run a cycle, False if one is running."""
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(os.path.join(STATE_DIR, "cycle.lock"), "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def monthly_cycle():
    with cycle_lock() as acquired:
        if not acquired:
            log.warning("monthly cycle skipped: another cycle is still running")
            return False
        started = time.monotonic()
        log.info("monthly cycle started")
        summary = {}
        for month in MONTHS:
            ok = call_job(INGESTION_URL, ["--month", month], f"ingest {month}")
            ok = ok and call_job(SPARK_URL, ["validate", "--month", month], f"validate {month}")
            summary[month] = "ok" if ok else "FAILED"
        log.info("monthly cycle finished in %.0fs: %s", time.monotonic() - started,
                 ", ".join(f"{m} {s}" for m, s in summary.items()))
        return all(s == "ok" for s in summary.values())


def quarterly_cycle():
    with cycle_lock() as acquired:
        if not acquired:
            log.warning("quarterly cycle skipped: another cycle is still running")
            return False
        started = time.monotonic()
        log.info("quarterly cycle started")
        summary = {q: "ok" if call_job(SPARK_URL, ["release", "--quarter", q], f"release {q}")
                   else "FAILED" for q in QUARTERS}
        log.info("quarterly cycle finished in %.0fs: %s", time.monotonic() - started,
                 ", ".join(f"{q} {s}" for q, s in summary.items()))
        return all(s == "ok" for s in summary.values())


def catch_up():
    """Run at start-up: process everything that has arrived so far."""
    if monthly_cycle():
        quarterly_cycle()
    else:
        log.warning("catch-up: quarterly cycle not started because the monthly cycle had failures")
