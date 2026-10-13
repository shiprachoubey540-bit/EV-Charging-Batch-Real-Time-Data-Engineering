"""
orchestration/dags/ev_pipeline_dag.py — Airflow DAG for the EV charging pipeline.

Owner: Shipra Choubey

Orchestrates the end-to-end pipeline:
  1. Extract raw data (ingestion)
  2. Transform with Spark (processing)
  3. Load into PostgreSQL (database)
  4. Run data-quality checks
"""

from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.operators.python import PythonOperator

DAG_ID = "ev_charging_pipeline"
DEFAULT_ARGS = {
    "owner": "shipra",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _extract():
    """Call ingestion.extract."""
    # TODO: Import and call ingestion.extract.extract_sessions()
    pass


def _transform():
    """Call processing.batch_transform."""
    # TODO: Import and call processing logic
    pass


def _load():
    """Call ingestion.load."""
    # TODO: Import and call ingestion.load.load_sessions()
    pass


def _quality_check():
    """Run data-quality validation."""
    # TODO: Execute database/quality_checks.sql
    pass


# Uncomment when Airflow is available:
# with DAG(
#     dag_id=DAG_ID,
#     default_args=DEFAULT_ARGS,
#     schedule_interval="@daily",
#     catchup=False,
#     tags=["ev-charging"],
# ) as dag:
#     extract_task = PythonOperator(task_id="extract", python_callable=_extract)
#     transform_task = PythonOperator(task_id="transform", python_callable=_transform)
#     load_task = PythonOperator(task_id="load", python_callable=_load)
#     quality_task = PythonOperator(task_id="quality_check", python_callable=_quality_check)
#
#     extract_task >> transform_task >> load_task >> quality_task
