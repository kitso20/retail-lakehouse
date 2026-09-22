import json
import os
from unittest.mock import MagicMock, patch

from include.load.postgres_load import (
    build_silver_row,
    get_connection,
    insert_drift_log,
    insert_silver_records,
)


def _record(**overrides):
    base = {
        "vendor": "spaza_sim",
        "product_name": "Bread 700g",
        "price_rand": 18.99,
        "discounted_price_rand": None,
        "original_price_rand": None,
        "extras": {"shop_area": "Soweto"},
        "schema_drift_fields": [],
        "observed_at": "2026-09-15T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_canonical_field_without_a_column_is_folded_into_extras_not_dropped():
    # loyalty_discount_pct is a mapped canonical field, but silver has no
    # column for it. The load layer must still not lose it.
    record = _record(loyalty_discount_pct=10)
    row = build_silver_row(record)

    extras = json.loads(row[5])  # extras column
    assert extras["loyalty_discount_pct"] == 10
    assert extras["shop_area"] == "Soweto"   # pre-existing extras preserved


def test_standard_row_keeps_column_order_and_types():
    row = build_silver_row(_record())
    vendor, product, price, discounted, original, extras_json, drift, observed_at = row
    assert (vendor, product, price) == ("spaza_sim", "Bread 700g", 18.99)
    assert discounted is None and original is None
    assert json.loads(extras_json) == {"shop_area": "Soweto"}
    assert drift == []
    assert observed_at == "2026-09-15T00:00:00+00:00"


def test_caller_supplied_extras_value_wins_over_canonical_key():
    # If the same field somehow exists both as extras and canonically,
    # we don't overwrite what's already recorded.
    record = _record(shop_area="Khayelitsha")
    extras = json.loads(build_silver_row(record)[5])
    assert extras["shop_area"] == "Soweto"


# ---- get_connection: env-driven warehouse wiring ----

def test_get_connection_passes_env_to_postgres():
    with patch("include.load.postgres_load.psycopg2.connect") as mock_connect, \
            patch.dict(os.environ, {
                "WAREHOUSE_DB_HOST": "db.example.com",
                "WAREHOUSE_DB_PORT": "5432",
                "WAREHOUSE_DB_NAME": "retail",
                "WAREHOUSE_DB_USER": "ci_user",
                "WAREHOUSE_DB_PASSWORD": "secret",
            }):
        get_connection()
    mock_connect.assert_called_once_with(
        host="db.example.com",
        port="5432",
        dbname="retail",
        user="ci_user",
        password="secret",
    )


# ---- insert_silver_records: append-only event log ----

def _mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur


def test_insert_silver_records_executes_and_commits():
    conn, cur = _mock_conn()
    records = [_record(), _record(product_name="Milk 1L")]

    with patch("include.load.postgres_load.execute_values") as mock_exec:
        count = insert_silver_records(records, conn)

    assert count == 2
    sql, rows = mock_exec.call_args.args[1], mock_exec.call_args.args[2]
    assert "INSERT INTO silver.product_price" in sql
    assert len(rows) == 2
    assert rows[0][0] == "spaza_sim" and rows[0][1] == "Bread 700g"
    conn.commit.assert_called_once()


def test_insert_silver_records_empty_short_circuits():
    conn, _ = _mock_conn()
    with patch("include.load.postgres_load.execute_values") as mock_exec:
        assert insert_silver_records([], conn) == 0
    mock_exec.assert_not_called()
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()


# ---- insert_drift_log: all six columns of the drift event ----

DRIFT_EVENT = {
    "vendor": "spaza_sim",
    "field": "loyalty_discount_pct",
    "kind": "new_field",
    "old_type": None,
    "new_type": "int",
    "detected_at": "2026-09-21T00:00:00+00:00",
}


def test_insert_drift_log_maps_all_six_columns():
    conn, _ = _mock_conn()
    with patch("include.load.postgres_load.execute_values") as mock_exec:
        count = insert_drift_log([DRIFT_EVENT], conn)

    assert count == 1
    sql, rows = mock_exec.call_args.args[1], mock_exec.call_args.args[2]
    assert "INSERT INTO silver.schema_drift_log" in sql
    assert rows == [(
        "spaza_sim", "loyalty_discount_pct", "new_field",
        None, "int", "2026-09-21T00:00:00+00:00",
    )]
    conn.commit.assert_called_once()


def test_insert_drift_log_empty_short_circuits():
    conn, _ = _mock_conn()
    with patch("include.load.postgres_load.execute_values") as mock_exec:
        assert insert_drift_log([], conn) == 0
    mock_exec.assert_not_called()
    conn.cursor.assert_not_called()
    conn.commit.assert_not_called()
