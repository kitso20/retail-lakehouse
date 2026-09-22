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

# Columns silver.product_price actually has (keep in sync with sql/schema.sql).
_SILVER_COLUMNS = {
    "vendor", "product_name", "price_rand", "discounted_price_rand",
    "original_price_rand", "extras", "schema_drift_fields", "observed_at",
}
# Bookkeeping keys that belong to the row itself, never to extras.
_RESERVED_KEYS = {"id", "ingested_at"}


def build_silver_row(r: dict) -> tuple:
    """
    Map a harmonized record onto silver.product_price's column order.

    Any CANONICAL field the table doesn't have a column for (e.g. the
    spaza simulator's loyalty_discount_pct, which has no dedicated
    column) is folded into `extras` instead of being dropped — the
    silver layer's promise is "nothing silently lost". Exported as a
    pure function so tests can assert that promise without a database.
    """
    extras = dict(r.get("extras") or {})
    for key, value in r.items():
        if key not in _SILVER_COLUMNS and key not in _RESERVED_KEYS and key not in extras:
            extras[key] = value

    return (
        r["vendor"],
        r["product_name"],
        r["price_rand"],
        r.get("discounted_price_rand"),
        r.get("original_price_rand"),
        json.dumps(extras),
        r.get("schema_drift_fields", []),
        r["observed_at"],
    )


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

    rows = [build_silver_row(r) for r in records]

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
