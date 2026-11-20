"""Tests for the ingestion logic, using an in-memory stand-in for S3.

Run from the repository root (no extra packages needed):
    python3 ingestion/tests/test_ingest.py
"""
import importlib.util
import io
import json
import os
import sys
import tempfile
import types
from pathlib import Path

# ---- stub boto3 / botocore so ingest.py can be imported without them
class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code); self.response = {"Error": {"Code": code}}
botocore = types.ModuleType("botocore"); bc = types.ModuleType("botocore.config"); be = types.ModuleType("botocore.exceptions")
bc.Config = lambda **k: None; be.ClientError = ClientError
sys.modules.update({"boto3": types.ModuleType("boto3"), "botocore": botocore, "botocore.config": bc, "botocore.exceptions": be})

spec = importlib.util.spec_from_file_location("ingest", Path(__file__).resolve().parent.parent / "ingest.py")
ing = importlib.util.module_from_spec(spec); spec.loader.exec_module(ing)

class FakeS3:
    def __init__(self): self.store = {}; self.fail_marker = False
    def put_object(self, Bucket, Key, Body, ContentType=None):
        if self.fail_marker and Key.endswith("_ingest.json"): raise RuntimeError("simulated crash before marker")
        self.store[(Bucket, Key)] = (Body, {})
    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store: raise ClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.store[(Bucket, Key)][0])}
    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store: raise ClientError("404")
        b, m = self.store[(Bucket, Key)]; return {"ContentLength": len(b), "Metadata": m}
    def upload_file(self, path, Bucket, Key, ExtraArgs):
        self.store[(Bucket, Key)] = (Path(path).read_bytes(), ExtraArgs["Metadata"])

os.chdir(tempfile.mkdtemp())
data = Path("data"); data.mkdir(exist_ok=True)
good = '"Date/Time","Lat","Lon","Base"\n"4/1/2014 0:11:00",40.769,-73.9549,"B02512"\n"4/30/2014 23:22:00",40.7652,-73.9876,"B02764"\n"5/1/2014 0:01:00",40.75,-73.98,"B02512"\n"garbage",40.7,-73.9,"B02512"\n'
(data / "uber-raw-data-apr14.csv").write_text(good)
s3 = FakeS3()

def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name); assert cond

r = ing.ingest_month(s3, "2014-04", "data")
check("first run ingests", r["outcome"] == "ingested" and r["rows"] == 4)
check("outside-month and unreadable rows counted", r["rows_outside_month"] == 1 and r["rows_unreadable_timestamp"] == 1)
check("time range", r["first_pickup"] == "2014-04-01T00:11:00" and r["last_pickup"] == "2014-05-01T00:01:00")
check("marker written", ("raw", "year=2014/month=04/_ingest.json") in s3.store)
state = json.loads(s3.store[("pipeline-state", "ingestion/2014-04.json")][0])
check("state succeeded", state["status"] == "succeeded")

r = ing.ingest_month(s3, "2014-04", "data")
check("rerun skips identical file", r["outcome"].startswith("skipped"))

(data / "uber-raw-data-apr14.csv").write_text(good + '"4/2/2014 1:00:00",40.7,-73.9,"B02512"\n')
try: ing.ingest_month(s3, "2014-04", "data"); ok = False
except ing.IngestionError: ok = True
check("different file is refused", ok)
state = json.loads(s3.store[("pipeline-state", "ingestion/2014-04.json")][0])
check("failure recorded in state", state["status"] == "failed" and "never overwritten" in state["error"])

# crash after upload, before marker; then a rerun completes
(data / "uber-raw-data-may14.csv").write_text(good.replace("4/", "5/"))
s3.fail_marker = True
try: ing.ingest_month(s3, "2014-05", "data")
except RuntimeError: pass
check("no marker after crash", ("raw", "year=2014/month=05/_ingest.json") not in s3.store)
s3.fail_marker = False
r = ing.ingest_month(s3, "2014-05", "data")
check("rerun after crash completes", r["outcome"] == "ingested")

(data / "uber-raw-data-jun14.csv").write_text("a,b,c\n1,2,3\n")
try: ing.ingest_month(s3, "2014-06", "data"); ok = False
except ing.IngestionError as e: ok = "unexpected header" in str(e)
check("wrong header refused", ok)

try: ing.ingest_month(s3, "2014-07", "data"); ok = False
except ing.IngestionError as e: ok = "not found" in str(e)
check("missing file refused", ok)
print("\nall tests passed")
