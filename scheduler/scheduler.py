"""Scheduler service: starts the pipeline cycles on a timetable (APScheduler).

    scheduler.py                    run the scheduler (container default)
    scheduler.py run-now monthly    run one cycle immediately, then exit
    scheduler.py run-now quarterly

Timetable (UTC, set in .env):
    MONTHLY_CRON    default "0 2 1 * *"          02:00 on the 1st of each month
    QUARTERLY_CRON  default "0 4 1 1,4,7,10 *"   04:00 on the 1st of each quarter
With RUN_ON_START=true a catch-up run (monthly, then quarterly) starts as
soon as the scheduler starts, so a fresh deployment processes all data.

The timetable is kept in a SQLite job store on a Docker volume, so it
survives restarts. A run missed while the scheduler was down is started
once after it comes back (within MISFIRE_GRACE seconds). Only one run of a
job at a time. The scheduler holds no storage keys: it only calls the
two job workers.
"""
import os
import sys
import threading

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import pipeline
from pipeline import log

MONTHLY_CRON = os.environ.get("MONTHLY_CRON", "0 2 1 * *")
QUARTERLY_CRON = os.environ.get("QUARTERLY_CRON", "0 4 1 1,4,7,10 *")
RUN_ON_START = os.environ.get("RUN_ON_START", "true").lower() == "true"
MISFIRE_GRACE = int(os.environ.get("MISFIRE_GRACE", str(6 * 3600)))


def ensure_job(scheduler, job_id, func, cron):
    """Adds the job, or updates its timetable if the cron setting changed."""
    trigger = CronTrigger.from_crontab(cron, timezone="UTC")
    job = scheduler.get_job(job_id)
    if job is None:
        scheduler.add_job(func, trigger, id=job_id, name=job_id)
        action = "added"
    elif str(job.trigger) != str(trigger):
        scheduler.reschedule_job(job_id, trigger=trigger)
        action = "timetable changed"
    else:
        action = "restored from job store"
    log.info("job %-9s %s, cron '%s', next run %s", job_id, action, cron,
             scheduler.get_job(job_id).next_run_time)


def serve():
    scheduler = BackgroundScheduler(
        jobstores={"default": SQLAlchemyJobStore(
            url=f"sqlite:///{pipeline.STATE_DIR}/jobs.sqlite")},
        job_defaults={"coalesce": True, "max_instances": 1,
                      "misfire_grace_time": MISFIRE_GRACE},
        timezone="UTC")
    scheduler.start(paused=True)
    ensure_job(scheduler, "monthly", pipeline.monthly_cycle, MONTHLY_CRON)
    ensure_job(scheduler, "quarterly", pipeline.quarterly_cycle, QUARTERLY_CRON)
    if RUN_ON_START:
        # A one-off job, kept in memory only so it does not repeat after a restart
        scheduler.add_jobstore("memory", alias="once")
        scheduler.add_job(pipeline.catch_up, id="catch-up", name="catch-up", jobstore="once")
        log.info("catch-up run starts now")
    scheduler.resume()
    log.info("scheduler running (retry delays %s s)", pipeline.retry_delays())
    try:
        threading.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "run-now" and sys.argv[2] in ("monthly", "quarterly"):
        ok = pipeline.monthly_cycle() if sys.argv[2] == "monthly" else pipeline.quarterly_cycle()
        return 0 if ok else 1
    if len(sys.argv) == 1:
        serve()
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
