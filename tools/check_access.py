"""Checks that every S3 identity can do exactly what it should, and no more.

Run with:  docker compose --profile check run --rm access-check
Exit code 0 means every check passed.
"""
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://s3:8333")
PROBE_KEY = "_access_check/probe.txt"


def client(prefix):
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ[f"S3_{prefix}_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"S3_{prefix}_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def attempt(s3, op, bucket):
    try:
        if op == "write":
            s3.put_object(Bucket=bucket, Key=PROBE_KEY, Body=b"probe")
        else:
            s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return True
    except ClientError:
        return False


# (identity, operation, bucket, expected result)
CASES = [
    ("INGEST", "write", "raw", True),
    ("INGEST", "write", "published", False),
    ("INGEST", "list", "processed", False),
    ("SPARK", "list", "raw", True),
    ("SPARK", "write", "raw", False),
    ("SPARK", "write", "processed", True),
    ("SPARK", "write", "quarantine", True),
    ("SPARK", "write", "published", True),
    ("APP", "list", "published", True),
    ("APP", "write", "published", False),
    ("APP", "list", "raw", False),
    ("APP", "list", "quarantine", False),
]

failures = 0
print(f"{'identity':<8} {'action':<6} {'bucket':<11} {'expected':<9} result")
for who, op, bucket, expected in CASES:
    allowed = attempt(client(who), op, bucket)
    ok = allowed == expected
    failures += not ok
    print(f"{who:<8} {op:<6} {bucket:<11} {'allow' if expected else 'deny':<9} "
          f"{'allow' if allowed else 'deny'}  {'OK' if ok else 'FAIL'}")

# Remove probe objects with the operator identity
admin = client("ADMIN")
for bucket in ("raw", "processed", "quarantine", "published"):
    try:
        admin.delete_object(Bucket=bucket, Key=PROBE_KEY)
    except ClientError:
        pass

print("\nAll access checks passed." if failures == 0 else f"\n{failures} check(s) failed.")
sys.exit(1 if failures else 0)
