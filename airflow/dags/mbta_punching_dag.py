# airflow/dags/mbta_bunching_dag.py

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mbta_bunching.ingest_vehicles import run_ingestion
from mbta_bunching.pipeline_io import (
    transform_latest_snapshot_to_silver,
    compute_gold_from_latest_silver,
)

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mbta_bus_bunching_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="*/15 * * * *",  # every 15 minutes
    catchup=False,
    description="End-to-end MBTA bus bunching pipeline (Bronze → Silver → Gold)",
) as dag:

    ingest = PythonOperator(
        task_id="ingest_vehicles_snapshot",
        python_callable=run_ingestion,
        op_kwargs={"routes": None},  # None => all bus routes
    )

    to_silver = PythonOperator(
        task_id="transform_latest_snapshot_to_silver",
        python_callable=transform_latest_snapshot_to_silver,
    )

    to_gold = PythonOperator(
        task_id="compute_headways_gold",
        python_callable=compute_gold_from_latest_silver,
    )

    ingest >> to_silver >> to_gold