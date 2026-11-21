"""Helpers shared by the PySpark jobs: storage client, JSON markers, Spark session."""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

NOT_FOUND = {"404", "NoSuchKey", "NotFound"}
STATE_BUCKET = "pipeline-state"


def get_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=f"%(asctime)s %(levelname)s {name} %(message)s",
    )
    return logging.getLogger(name)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def read_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in NOT_FOUND:
            return None
        raise


def write_json(s3, bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        ContentType="application/json",
        Body=json.dumps(data, indent=2).encode(),
    )


def delete_if_exists(s3, bucket, key):
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] not in NOT_FOUND:
            raise


def spark_session(app_name):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def short_error(exc):
    """The most useful single line of an error, for logs and summaries."""
    lines = [l.strip() for l in str(exc).splitlines() if l.strip()]
    for line in lines:
        if "Exception" in line or "Error:" in line:
            return line.lstrip(": ")[:200]
    return (lines[0] if lines else type(exc).__name__)[:200]
