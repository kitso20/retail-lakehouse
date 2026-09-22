"""
DAG: retail_lakehouse_pipeline

Extract (4 simulated vendors) -> land raw JSON to S3/MinIO bronze zone
-> harmonize into canonical silver schema (schema registry catches
drift) in the `transform` task -> write silver + drift log to Postgres
in the `load` task. Gold layer is built separately by dbt (see dbt/
folder) reading from silver.product_price.

The transform/load split is deliberate: transform is pure computation
(bronze + registry, no database), load owns all database writes, and
the rows travel between them via XCom (~170 rows/day — small enough
that pushing them is simpler than staging them somewhere).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.extract.vendors.formal_retail_simulator import (
    ShopriteSimConnector, PnpSimConnector, WoolworthsSimConnector,
)
from include.extract.vendors.spaza_simulator import SpazaSimulatorConnector
from include.extract.land_raw import land_bronze, read_bronze
from include.transform.schema_registry import SchemaRegistry
from include.transform.harmonize import harmonize_batch
from include.load.postgres_load import get_connection, insert_silver_records, insert_drift_log

default_args = {"owner": "you", "retries": 3, "retry_delay": timedelta(minutes=2)}

VENDOR_CONNECTORS = {
    "shoprite_sim": ShopriteSimConnector,
    "pnp_sim": PnpSimConnector,
    "woolworths_sim": WoolworthsSimConnector,
    "spaza_sim": SpazaSimulatorConnector,
}

# Persisted so the schema registry remembers known fields ACROSS DAG
# runs, not just within one run — otherwise every run would think
# every field is "new."
REGISTRY_STORE_PATH = "/opt/airflow/dags/schema_registry_state.json"


def _extract(**context):
    """Run every vendor connector, land each vendor's batch to bronze."""
    today = context["ds"]  # Airflow's logical date, YYYY-MM-DD
    for vendor_name, connector_cls in VENDOR_CONNECTORS.items():
        connector = connector_cls()
        raw_records = connector.fetch_raw_products()
        envelope = connector.to_bronze_envelope(raw_records)
        land_bronze(vendor=vendor_name, envelope_records=envelope, dt=today)
    context["ti"].xcom_push(key="run_date", value=today)


def _transform(**context):
    """Read today's bronze files, harmonize into silver rows, hand off.

    No database connection here on purpose: transforming is pure
    computation over bronze + registry, and the result travels to the
    load task through XCom.
    """
    ti = context["ti"]
    today = ti.xcom_pull(key="run_date", task_ids="extract")
    # EVENT time for silver.observed_at: the run's logical date, so a
    # backfill of last month stamps last month (see harmonize_record).
    observed_at = context["logical_date"].isoformat()
    registry = SchemaRegistry(store_path=REGISTRY_STORE_PATH)

    silver_rows = []
    for vendor_name in VENDOR_CONNECTORS:
        bronze_records = read_bronze(vendor=vendor_name, dt=today)
        raw_payloads = [r["raw_payload"] for r in bronze_records]
        silver_rows.extend(
            harmonize_batch(vendor_name, raw_payloads, registry, observed_at=observed_at)
        )

    drift_events = registry.drift_report()
    ti.xcom_push(key="silver_rows", value=silver_rows)
    ti.xcom_push(key="drift_events", value=drift_events)
    print(f"Transformed {len(silver_rows)} silver rows, {len(drift_events)} "
          f"schema drift events - handing off to load.")


def _load(**context):
    """Write the transform task's output to Postgres (append-only)."""
    silver_rows = context["ti"].xcom_pull(key="silver_rows", task_ids="transform") or []
    drift_events = context["ti"].xcom_pull(key="drift_events", task_ids="transform") or []

    conn = get_connection()
    try:
        total_rows = insert_silver_records(silver_rows, conn)
        total_drift = insert_drift_log(drift_events, conn)
    finally:
        conn.close()

    print(f"Loaded {total_rows} silver rows, logged {total_drift} schema drift events.")


with DAG(
    dag_id="retail_lakehouse_pipeline",
    description="Multi-vendor SA retail pricing lakehouse (simulated data)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    # One run at a time: backfills replay day-by-day instead of fanning
    # out, which keeps vendor API pressure (and surprises) predictable.
    max_active_runs=1,
    tags=["portfolio", "data-engineering", "lakehouse"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    load = PythonOperator(task_id="load", python_callable=_load)

    extract >> transform >> load
