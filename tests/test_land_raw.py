"""
Bronze layer tests: S3 key layout, JSONL body, env-driven client config.

The S3 client is replaced with a mock — these tests assert the CONTRACT
(Hive-style partition keys, logical-date partitioning, blank-endpoint
passthrough to real AWS) without needing MinIO or AWS.
"""
import json
import os
import re
from unittest.mock import MagicMock, patch

from include.extract import land_raw


def _client_ctx(s3):
    """Patch get_s3_client inside the module under test."""
    return patch.object(land_raw, "get_s3_client", return_value=s3)


# ---- get_s3_client: endpoint/region wiring (the local-vs-AWS switch) ----

def test_blank_endpoint_falls_through_to_real_aws():
    """Blank S3_ENDPOINT_URL must mean 'no endpoint_url', not MinIO."""
    with patch("include.extract.land_raw.boto3.client") as mock_boto, \
            patch.dict(os.environ, {
                "S3_ENDPOINT_URL": "",
                "AWS_ACCESS_KEY_ID": "AKIATEST",
                "AWS_SECRET_ACCESS_KEY": "secret",
            }):
        os.environ.pop("AWS_REGION", None)
        land_raw.get_s3_client()
    mock_boto.assert_called_once_with(
        "s3",
        endpoint_url=None,
        aws_access_key_id="AKIATEST",
        aws_secret_access_key="secret",
        region_name="af-south-1",
    )


def test_endpoint_passthrough_when_set():
    with patch("include.extract.land_raw.boto3.client") as mock_boto, \
            patch.dict(os.environ, {"S3_ENDPOINT_URL": "http://minio:9000"}):
        land_raw.get_s3_client()
    assert mock_boto.call_args.kwargs["endpoint_url"] == "http://minio:9000"


def test_region_env_override():
    with patch("include.extract.land_raw.boto3.client") as mock_boto, \
            patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}):
        land_raw.get_s3_client()
    assert mock_boto.call_args.kwargs["region_name"] == "eu-west-1"


# ---- land_bronze: key layout + JSONL body ----

def test_key_uses_logical_date_partition_not_wall_clock():
    s3 = MagicMock()
    records = [{"vendor": "shoprite_sim", "raw_payload": {"a": 1}},
               {"vendor": "shoprite_sim", "raw_payload": {"b": 2}}]
    with _client_ctx(s3):
        key = land_raw.land_bronze(
            vendor="shoprite_sim", envelope_records=records,
            bucket="test-bucket", dt="2026-09-21",
        )

    assert re.fullmatch(
        r"vendor=shoprite_sim/dt=2026-09-21/\d{8}T\d{6}Z\.json", key
    ), key

    s3.put_object.assert_called_once()
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Key"] == key
    lines = kwargs["Body"].decode("utf-8").splitlines()
    assert [json.loads(line) for line in lines] == records


def test_bucket_and_dt_default_from_env_and_now():
    s3 = MagicMock()
    with patch.dict(os.environ, {"S3_BUCKET": "env-bucket"}), _client_ctx(s3):
        key = land_raw.land_bronze(vendor="pnp_sim", envelope_records=[])
    assert key.startswith("vendor=pnp_sim/dt=")
    assert re.search(r"/dt=\d{4}-\d{2}-\d{2}/\d{8}T\d{6}Z\.json$", key)
    assert s3.put_object.call_args.kwargs["Bucket"] == "env-bucket"


# ---- read_bronze: prefix + multi-page JSONL parse ----

def test_reads_all_pages_and_parses_jsonl():
    s3 = MagicMock()
    paginator = s3.get_paginator.return_value
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "vendor=v/dt=2026-09-21/f1.json"}]},
        {"Contents": [{"Key": "vendor=v/dt=2026-09-21/f2.json"}]},
        {},  # page without Contents must not crash
    ]
    body1 = MagicMock()
    body1.read.return_value = b'{"raw_payload": {"n": 1}}\n\n{"raw_payload": {"n": 2}}\n'
    body2 = MagicMock()
    body2.read.return_value = b'{"raw_payload": {"n": 3}}\n'
    s3.get_object.side_effect = [{"Body": body1}, {"Body": body2}]

    with _client_ctx(s3):
        records = land_raw.read_bronze(vendor="v", dt="2026-09-21", bucket="b")

    s3.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(
        Bucket="b", Prefix="vendor=v/dt=2026-09-21/"
    )
    assert [r["raw_payload"]["n"] for r in records] == [1, 2, 3]


def test_empty_partition_returns_no_records():
    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [{}]
    with _client_ctx(s3):
        assert land_raw.read_bronze(vendor="v", dt="1999-01-01") == []
