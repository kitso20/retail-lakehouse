"""
Load: write silver.product_price rows + silver.schema_drift_log rows
into Postgres. No upsert/ON CONFLICT here on purpose — every record is
a point-in-time price observation, so we always append (like an
event log), we don't overwrite history. That's a deliberate design
choice worth being able to explain: "why append instead of upsert
like the weather pipeline?" — because weather has one true current
value per city+timestamp, but pricing history is something you WANT
to keep every observation of, to track price changes over time.
"""
import json
import os

import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_DB_HOST", "localhost"),
        port=os.environ.get("WAREHOUSE_DB_PORT", "5433"),
        dbname=os.environ.get("WAREHOUSE_DB_NAME", "retail"),
        user=os.environ.get("WAREHOUSE_DB_USER", "retail_user"),
        password=os.environ.get("WAREHOUSE_DB_PASSWORD", "retail_pass"),
    )


def insert_silver_records(records: list[dict], conn) -> int:
    if not records:
        return 0

    rows = [
        (
            r["vendor"],
            r["product_name"],
            r["price_rand"],
            r.get("discounted_price_rand"),
            r.get("original_price_rand"),
            json.dumps(r.get("extras", {})),
            r.get("schema_drift_fields", []),
            r["observed_at"],
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO silver.product_price
                (vendor, product_name, price_rand, discounted_price_rand,
                 original_price_rand, extras, schema_drift_fields, observed_at)
            VALUES %s
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def insert_drift_log(drift_events: list[dict], conn) -> int:
    if not drift_events:
        return 0

    rows = [
        (e["vendor"], e["field"], e["kind"], e["old_type"], e["new_type"], e["detected_at"])
        for e in drift_events
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO silver.schema_drift_log
                (vendor, field, kind, old_type, new_type, detected_at)
            VALUES %s
            """,
            rows,
        )
        conn.commit()
    return len(rows)
