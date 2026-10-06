from datetime import datetime, timedelta
from typing import Any, Dict
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.extract import run_extract
from pipeline.transform import run_transform
from pipeline.load import run_load
from pipeline.utils import airflow_on_failure_callback

# Centralized default operational parameters for DAG execution
DEFAULT_ARGS = {
    "owner": "dataforge",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Operational resilience: retry up to 3 times with exponential backoff
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    # Automated auditing and paging hook on failure execution states
    "on_failure_callback": airflow_on_failure_callback,
}

# Define the orchestration wrapper
with DAG(
    dag_id="dataforge_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end meteorological data ingestion, transformation, and load pipeline.",
    schedule_interval="0 * * * *",  # Execute hourly
    start_date=datetime(2023, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataforge", "etl", "weather", "production"],
    doc_md="""
    # DataForge ELT Pipeline Orchestration
    This DAG manages the end-to-end execution flow of the DataForge pipeline:
    1. **Extract**: Pulls raw meteorological metrics from the configured public API and registers them in MinIO/S3.
    2. **Transform**: Extracts, cleans, deduplicates via Pandas, validates schema structures with Pydantic, and serializes clean Parquet storage.
    3. **Load**: Re-integrates Parquet buffers into relational PostgreSQL tables via ACID transactional upserts.
    """,
) as dag:

    # 1. Extraction Stage
    # Executes API extraction and tracks raw S3 path outputs.
    extract_task = PythonOperator(
        task_id="extract_raw_data",
        python_callable=run_extract,
        op_kwargs={"execution_date_str": "{{ ts }}"},
        sla=timedelta(minutes=5),
    )

    # 2. Transformation Stage
    # Consumes output path metadata, normalizes schema and values, and validates constraints.
    transform_task = PythonOperator(
        task_id="transform_raw_data",
        python_callable=run_transform,
        op_kwargs={
            "raw_s3_key": "{{ task_instance.xcom_pull(task_ids='extract_raw_data') }}"
        },
        sla=timedelta(minutes=10),
    )

    # 3. PostgreSQL Load Stage
    # Performs transactional merge (UPSERT) on constraints, and writes operational metrics to audit tables.
    load_task = PythonOperator(
        task_id="load_clean_data",
        python_callable=run_load,
        op_kwargs={
            "clean_s3_key": "{{ task_instance.xcom_pull(task_ids='transform_raw_data') }}"
        },
        sla=timedelta(minutes=15),
    )

    # Declarative execution graph
    extract_task >> transform_task >> load_task
