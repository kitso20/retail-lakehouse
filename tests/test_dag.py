"""
DAG wiring tests: task ids, schedule, backfill-safety limits, callables.

Two layers, because CI has no airflow installed but the dockerized
runner does:
- AST checks parse dags/retail_dag.py WITHOUT importing airflow, so
  they run everywhere — including guarding the task ids (extract,
  transform, load — never the old combined transform_and_load).
- Callable/structural checks import the real DAG and exercise the task
  bodies with mocked S3/Postgres; they skip cleanly where airflow is
  missing.
"""
import ast
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "retail_dag.py"


def _tree() -> ast.Module:
    return ast.parse(DAG_PATH.read_text(encoding="utf-8"))


def _task_ids(tree: ast.Module) -> list[str]:
    ids = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                    ids.append(kw.value.value)
    return ids


def _dag_kwargs(tree: ast.Module) -> dict:
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "DAG"):
            return {kw.arg: kw.value for kw in node.keywords}
    raise AssertionError("no DAG(...) call found in dags/retail_dag.py")


def _load_dag_module():
    """Import the real DAG module (docker runner only — needs airflow)."""
    pytest.importorskip("airflow", reason="airflow only installed in the docker image")
    spec = importlib.util.spec_from_file_location("retail_dag_under_test", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---- AST layer: runs everywhere, no airflow needed ----

def test_task_ids_are_extract_transform_load():
    source = DAG_PATH.read_text(encoding="utf-8")
    ids = _task_ids(_tree())
    assert ids.count("extract") == 1
    assert ids.count("transform") == 1
    assert ids.count("load") == 1
    # Regression guards: neither the original combined id nor the
    # dashed interim form may come back.
    for stale in ("transform_and_load", "transform-and-load", "transform_load"):
        assert stale not in ids
        assert stale not in source


def test_dag_is_backfill_safe():
    kwargs = _dag_kwargs(_tree())
    assert ast.literal_eval(kwargs["schedule"]) == "@daily"
    assert ast.literal_eval(kwargs["catchup"]) is False
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1


def test_extract_transform_load_chain():
    """`extract >> transform >> load` must survive refactors."""

    def _chain_names(node) -> list:
        """Flatten a left-nested `a >> b >> c` into an ordered name list."""
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
            names = _chain_names(node.left)
            if isinstance(node.right, ast.Name):
                names.append(node.right.id)
            else:
                names.extend(_chain_names(node.right))
            return names
        return [node.id] if isinstance(node, ast.Name) else []

    pairs = set()
    for node in ast.walk(_tree()):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
            names = _chain_names(node)
            pairs.update(zip(names, names[1:]))
    assert ("extract", "transform") in pairs
    assert ("transform", "load") in pairs


# ---- structural layer: full Airflow import (docker runner only) ----

def test_dag_object_wires_up_in_airflow():
    module = _load_dag_module()

    dag = module.dag
    assert dag.dag_id == "retail_lakehouse_pipeline"
    assert {t.task_id for t in dag.tasks} == {"extract", "transform", "load"}

    transform = dag.get_task("transform")
    load = dag.get_task("load")
    assert [up.task_id for up in transform.upstream_list] == ["extract"]
    assert [up.task_id for up in load.upstream_list] == ["transform"]
    assert dag.get_task("extract").downstream_task_ids == {"transform"}
    assert transform.downstream_task_ids == {"load"}
    # Task-level settings inherited from default_args
    assert transform.retries == 3 and load.retries == 3


# ---- callable layer: the actual task bodies (S3/Postgres mocked) ----

def test_extract_lands_every_vendor_and_pushes_run_date():
    module = _load_dag_module()
    ti = MagicMock()

    with patch.object(module, "land_bronze") as mock_land:
        module._extract(ds="2026-09-21", ti=ti)

    calls = mock_land.call_args_list
    assert {c.kwargs["vendor"] for c in calls} == set(module.VENDOR_CONNECTORS)
    for call in calls:
        assert call.kwargs["dt"] == "2026-09-21"
        envelope = call.kwargs["envelope_records"]
        assert envelope, "connector produced no records"
        assert envelope[0]["vendor"] == call.kwargs["vendor"]
        assert "raw_payload" in envelope[0]
    ti.xcom_push.assert_called_once_with(key="run_date", value="2026-09-21")


def test_transform_uses_event_time_and_hands_off_via_xcom(tmp_path):
    module = _load_dag_module()

    logical_date = datetime(2026, 9, 21, tzinfo=timezone.utc)
    ti = MagicMock()
    ti.xcom_pull.return_value = "2026-09-21"
    bronze = [{"raw_payload": {"product_name": "Milk", "unit_price": 20.0}}]

    with patch.object(module, "read_bronze", return_value=bronze) as mock_read, \
            patch.object(module, "harmonize_batch", return_value=[{"vendor": "x"}]) as mock_harm, \
            patch.object(module, "get_connection") as mock_conn, \
            patch.object(module, "REGISTRY_STORE_PATH", str(tmp_path / "registry.json")):
        module._transform(ti=ti, logical_date=logical_date)

    # Transform is pure computation: bronze in, rows out, no DB touched.
    mock_conn.assert_not_called()
    ti.xcom_pull.assert_called_once_with(key="run_date", task_ids="extract")

    assert {c.kwargs["vendor"] for c in mock_read.call_args_list} == set(module.VENDOR_CONNECTORS)
    for c in mock_read.call_args_list:
        assert c.kwargs["dt"] == "2026-09-21"
    assert mock_harm.call_count == len(module.VENDOR_CONNECTORS)
    for c in mock_harm.call_args_list:
        # EVENT time comes from the logical date, per vendor, not the clock.
        assert c.kwargs["observed_at"] == "2026-09-21T00:00:00+00:00"
        assert c.args[1], "payloads must be forwarded to harmonize"

    pushed = {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}
    assert set(pushed) == {"silver_rows", "drift_events"}
    assert len(pushed["silver_rows"]) == len(module.VENDOR_CONNECTORS)
    assert pushed["drift_events"] == []


def test_load_writes_transform_output_and_closes_connection():
    module = _load_dag_module()

    ti = MagicMock()
    rows = [{"vendor": "spaza_sim", "product_name": "Bread 700g", "price_rand": 18.99}]
    events = [{"vendor": "spaza_sim", "field": "loyalty_discount_pct", "kind": "new_field",
               "old_type": None, "new_type": "int", "detected_at": "2026-09-21T00:00:00+00:00"}]

    def _pull(key, task_ids):
        assert task_ids == "transform", "load must read only from transform"
        return {"silver_rows": rows, "drift_events": events}[key]

    ti.xcom_pull.side_effect = _pull
    conn = MagicMock()

    with patch.object(module, "get_connection", return_value=conn), \
            patch.object(module, "insert_silver_records", return_value=1) as mock_silver, \
            patch.object(module, "insert_drift_log", return_value=1) as mock_drift:
        module._load(ti=ti)

    mock_silver.assert_called_once_with(rows, conn)
    mock_drift.assert_called_once_with(events, conn)
    conn.close.assert_called_once()  # finally: connection never leaks


def test_load_tolerates_missing_xcom_as_empty_batches():
    """Cleared/missing handoff must not crash — inserts just no-op."""
    module = _load_dag_module()

    ti = MagicMock()
    ti.xcom_pull.return_value = None
    conn = MagicMock()

    with patch.object(module, "get_connection", return_value=conn), \
            patch.object(module, "insert_silver_records", return_value=0) as mock_silver, \
            patch.object(module, "insert_drift_log", return_value=0) as mock_drift:
        module._load(ti=ti)

    mock_silver.assert_called_once_with([], conn)
    mock_drift.assert_called_once_with([], conn)
    conn.close.assert_called_once()
