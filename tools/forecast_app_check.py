"""Stand-in for the forecasting application: reads the published releases
through the S3 gateway on localhost, exactly as an external consumer would,
using the read-only key, and checks that this key cannot do anything else.

Run on your own machine, from the repository root:
    source .venv/bin/activate
    pip install "boto3~=1.35.0" pyarrow
    python tools/forecast_app_check.py

Exit code 0 means every release was readable and complete and every access
check behaved as designed.
"""
import io
import json
import os
import sys
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

QUARTERS = ["2014-Q2", "2014-Q3"]
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def setting(name):
    """Environment variable, or the value from .env."""
    if name in os.environ:
        return os.environ[name]
    for line in ENV_FILE.read_text().splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found in the environment or in .env")


s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:8333"),
    aws_access_key_id=setting("S3_APP_ACCESS_KEY"),
    aws_secret_access_key=setting("S3_APP_SECRET_KEY"),
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
)
failures = 0


def check(name, cond, detail=""):
    global failures
    print(f"{'PASS' if cond else 'FAIL'} {name}{'  ' + detail if detail else ''}")
    failures += not cond


def read_table(path):
    """Downloads every Parquet file under an s3://bucket/prefix/ path."""
    bucket, prefix = path[len("s3://"):].split("/", 1)
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
            if o["Key"].endswith(".parquet")]
    parts = [pq.read_table(io.BytesIO(s3.get_object(Bucket=bucket, Key=k)["Body"].read()))
             for k in keys]
    return pa.concat_tables(parts) if parts else None


print("Reading published releases with the forecasting app's read-only key\n")
for quarter in QUARTERS:
    try:
        manifest = json.loads(s3.get_object(
            Bucket="published", Key=f"quarter={quarter}/_manifest.json")["Body"].read())
    except ClientError as e:
        check(f"{quarter} manifest readable", False, e.response["Error"]["Code"])
        continue
    print(f"{quarter}: release {manifest['release_id']}, months {', '.join(manifest['months'])}")
    for name, info in manifest["tables"].items():
        table = read_table(info["path"])
        total = sum(table.column("pickups").to_pylist()) if table is not None else 0
        check(f"  {name}: {table.num_rows if table is not None else 0:,} rows, "
              f"{total:,} pickups", total == manifest["total_pickups"],
              f"(manifest says {manifest['total_pickups']:,})")
    busiest = read_table(manifest["tables"]["demand_by_cell_hour"]["path"])
    if busiest is not None:
        by_cell = {}
        for cell, n in zip(busiest.column("cell_id").to_pylist(),
                           busiest.column("pickups").to_pylist()):
            by_cell[cell] = by_cell.get(cell, 0) + n
        top = sorted(by_cell.items(), key=lambda kv: -kv[1])[:3]
        print("  busiest grid cells: " + ", ".join(f"{c} ({n:,})" for c, n in top))
    print()


def denied(action):
    try:
        action()
        return False
    except ClientError as e:
        return e.response["Error"]["Code"] in ("AccessDenied", "403")


print("Access checks for the read-only key")
check("cannot write to published",
      denied(lambda: s3.put_object(Bucket="published", Key="_probe.txt", Body=b"x")))
check("cannot list raw", denied(lambda: s3.list_objects_v2(Bucket="raw", MaxKeys=1)))
check("cannot read a raw file", denied(lambda: s3.get_object(
    Bucket="raw", Key="year=2014/month=04/_ingest.json")))
check("cannot list quarantine", denied(lambda: s3.list_objects_v2(Bucket="quarantine", MaxKeys=1)))

print("\nAll checks passed." if failures == 0 else f"\n{failures} check(s) failed.")
sys.exit(1 if failures else 0)
