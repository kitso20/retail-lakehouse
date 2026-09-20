"""
DAG: retail_lakehouse_pipeline

Extract (4 simulated vendors) -> land raw JSON to S3/MinIO bronze zone
-> harmonize into canonical silver schema (schema registry catches
drift) -> load silver + drift log into Postgres. Gold layer is built
separately by dbt (see dbt/ folder) reading from silver.product_price.
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
        land_bronze(vendor=vendor_name, envelope_records=envelope)
    context["ti"].xcom_push(key="run_date", value=today)


def _transform_and_load(**context):
    """Read back today's bronze files, harmonize, write to silver."""
    today = context["ti"].xcom_pull(key="run_date", task_ids="extract")
    registry = SchemaRegistry(store_path=REGISTRY_STORE_PATH)

    conn = get_connection()
    total_rows, total_drift = 0, 0
    try:
        for vendor_name in VENDOR_CONNECTORS:
            bronze_records = read_bronze(vendor=vendor_name, dt=today)
            raw_payloads = [r["raw_payload"] for r in bronze_records]

            silver_rows = harmonize_batch(vendor_name, raw_payloads, registry)
            total_rows += insert_silver_records(silver_rows, conn)

        drift_events = registry.drift_report()
        total_drift += insert_drift_log(drift_events, conn)
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
    tags=["portfolio", "data-engineering", "lakehouse"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform_and_load = PythonOperator(task_id="transform_and_load", python_callable=_transform_and_load)

    extract >> transform_and_load
