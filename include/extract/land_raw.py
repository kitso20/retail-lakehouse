"""
Bronze layer writer: raw records -> S3 (or MinIO locally), untouched.

Partition scheme: vendor=<name>/dt=<YYYY-MM-DD>/<timestamp>.json
This is the standard "Hive-style partitioning" pattern — it's what lets
a query engine (Athena, Spark, dbt) prune partitions instead of
scanning the entire bucket, and it's what makes "replay yesterday's
data" a one-line filter instead of a re-scrape.

Notice this function takes the SAME boto3 client whether it's pointed
at MinIO (local dev) or real AWS S3 (production) — only endpoint_url
and credentials differ, set via environment variables. That portability
is the actual reason companies use S3-compatible object storage for
local dev instead of mocking it entirely.
"""
import json
import os
from datetime import datetime, timezone

import boto3


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),  # None -> real AWS S3
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def land_bronze(vendor: str, envelope_records: list[dict], bucket: str | None = None) -> str:
    """
    Writes one JSON file containing all of this run's records for a
    vendor. Returns the S3 key written.

    Simple example of the resulting key:
    "vendor=shoprite/dt=2026-09-18/20260918T100301Z.json"
    """
    bucket = bucket or os.environ.get("S3_BUCKET", "retail-lakehouse")
    s3 = get_s3_client()

    now = datetime.now(timezone.utc)
    dt_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    key = f"vendor={vendor}/dt={dt_partition}/{timestamp}.json"

    body = "\n".join(json.dumps(record) for record in envelope_records)  # JSON Lines
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))

    print(f"[bronze] Wrote {len(envelope_records)} records to s3://{bucket}/{key}")
    return key


def read_bronze(vendor: str, dt: str, bucket: str | None = None) -> list[dict]:
    """
    Reads back every bronze file for a given vendor+date partition.
    Used by the silver transform step.
    """
    bucket = bucket or os.environ.get("S3_BUCKET", "retail-lakehouse")
    s3 = get_s3_client()
    prefix = f"vendor={vendor}/dt={dt}/"

    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
            records.extend(json.loads(line) for line in body.splitlines() if line.strip())
    return records
