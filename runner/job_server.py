"""Small HTTP front for a batch-job container.

The scheduler starts jobs by calling this server over the private Docker
network, so it never needs access to Docker itself (no Docker socket).

    GET  /health                           -> {"status": "ok", "busy": false}
    POST /run  {"args": ["--month", "2014-04"]}
         -> runs JOB_COMMAND + args, waits for it to finish and returns
            {"exit_code": 0, "seconds": 2.1, "output_tail": [...]}

Only one job runs at a time (409 while busy). Arguments must match a strict
pattern, and the command is started without a shell, so a caller cannot run
anything except this container's own job.
Uses only the Python standard library.
"""
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

COMMAND = shlex.split(os.environ["JOB_COMMAND"])
ALLOWED_ARG = re.compile(
    r"^(--[a-z][a-z-]*|validate|release|20\d\d-(0[1-9]|1[0-2])|20\d\d-Q[1-4])$")
TAIL_LINES = 40
busy = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        pass  # the jobs write their own logs

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok", "busy": busy.locked()})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/run":
            return self._send(404, {"error": "not found"})
        try:
            length = int(self.headers.get("Content-Length", 0))
            args = json.loads(self.rfile.read(length) or b"{}").get("args", [])
        except (ValueError, AttributeError):
            return self._send(400, {"error": "invalid request"})
        if not isinstance(args, list) or not all(
                isinstance(a, str) and ALLOWED_ARG.match(a) for a in args):
            return self._send(400, {"error": "arguments not allowed"})
        if not busy.acquire(blocking=False):
            return self._send(409, {"error": "a job is already running"})
        try:
            started = time.monotonic()
            proc = subprocess.run(COMMAND + args, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True)
            sys.stdout.write(proc.stdout)  # keep the job's log in `docker compose logs`
            sys.stdout.flush()
            self._send(200, {"exit_code": proc.returncode,
                             "seconds": round(time.monotonic() - started, 2),
                             "output_tail": proc.stdout.splitlines()[-TAIL_LINES:]})
        finally:
            busy.release()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    print(f"job server ready on port {port}: {' '.join(COMMAND)}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
