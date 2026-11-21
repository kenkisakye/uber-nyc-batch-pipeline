"""Tests for the scheduler's retry logic and the job server, using a fake job.

Runs on your own machine with plain Python (no packages, no Docker):
    python3 scheduler/tests/test_scheduler.py
"""

import atexit
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

R = str(Path(__file__).resolve().parents[2])
TMP = tempfile.mkdtemp()
FAKE_JOB = os.path.join(TMP, "fakejob.py")
with open(FAKE_JOB, "w") as f:
    f.write(
        'import sys, time\nargs = sys.argv[1:]\nprint("fake job", args)\nif "2014-09" in args: print("ERROR validation month=2014-09 failed: boom"); sys.exit(1)\nif "--slow" in args: time.sleep(2)\n'
    )
# A free port, so an earlier run cannot be in the way
with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    PORT = probe.getsockname()[1]
URL = f"http://127.0.0.1:{PORT}"
# shlex.join quotes paths that contain spaces
env = dict(
    os.environ, JOB_COMMAND=shlex.join([sys.executable, FAKE_JOB]), PORT=str(PORT)
)
servers = []


def start():
    proc = subprocess.Popen(
        [sys.executable, f"{R}/runner/job_server.py"],
        env=env,
        stdout=subprocess.DEVNULL,
    )
    servers.append(proc)
    return proc


# Always stop the test servers, also when a test fails
atexit.register(lambda: [p.terminate() for p in servers if p.poll() is None])
os.environ.update(
    RETRY_DELAYS="1,1",
    STATE_DIR=os.path.join(TMP, "state"),
    INGESTION_URL=URL,
    SPARK_URL=URL,
)
sys.path.insert(0, f"{R}/scheduler")
import pipeline


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    assert cond


srv = start()
time.sleep(0.8)
check("job succeeds", pipeline.call_job(URL, ["--month", "2014-04"], "ingest 2014-04"))
check(
    "failing job is retried then reported",
    not pipeline.call_job(URL, ["validate", "--month", "2014-09"], "validate 2014-09"),
)
req = urllib.request.Request(
    URL + "/run", method="POST", data=json.dumps({"args": ["; rm -rf /"]}).encode()
)
try:
    urllib.request.urlopen(req)
    bad = False
except urllib.error.HTTPError as e:
    bad = e.code == 400
check("unsafe argument rejected", bad)
# busy: one slow job holds the lock
t = threading.Thread(target=lambda: pipeline.call_job(URL, ["--slow"], "slow"))
t.start()
time.sleep(0.3)
req = urllib.request.Request(
    URL + "/run",
    method="POST",
    data=json.dumps({"args": ["--month", "2014-05"]}).encode(),
)
try:
    urllib.request.urlopen(req)
    busy = False
except urllib.error.HTTPError as e:
    busy = e.code == 409
check("second job while busy gets 409", busy)
t.join()
srv.terminate()
srv.wait()
# worker down, comes back during retries
# The worker comes back 1.3 s later, during the retries
threading.Thread(target=lambda: (time.sleep(1.3), start())).start()
check(
    "worker down, then back: retry succeeds",
    pipeline.call_job(URL, ["--month", "2014-04"], "ingest 2014-04"),
)
# lock: second cycle skipped while one runs
with pipeline.cycle_lock() as a:
    with pipeline.cycle_lock() as b:
        check("only one cycle at a time", a and not b)
check("monthly cycle reports the failing month", pipeline.monthly_cycle() is False)
print("all scheduler tests passed")
