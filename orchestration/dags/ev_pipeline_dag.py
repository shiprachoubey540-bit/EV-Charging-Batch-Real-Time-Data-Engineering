"""
orchestration/dags/ev_pipeline_dag.py — Airflow DAG for the EV charging pipeline.

Owner: Shipra Choubey

Orchestrates the end-to-end historical batch ETL workflow:
  1. extract: Ingests raw JSON documents (full candidate or sample fallback).
  2. validate: Enforces schema contracts, identifies duplicates & anomaly flags.
  3. transform: Normalizes into structured session and user-input records.
  4. load: Writes clean tabular CSV/Parquet artifacts and quality reports.
  5. quality_check: Executes post-load integrity validation & foreign-key audits.

Cross-Platform Compatibility:
  Uses official Apache Airflow classes when installed (Linux/Docker/WSL).
  Provides a standalone execution shim on native Windows for local testing and pytest.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.extract import extract_sessions
from ingestion.validate import validate_sessions
from ingestion.transform import transform_sessions
from ingestion.load import load_sessions

logger = logging.getLogger("orchestration.ev_pipeline_dag")

# ----------------------------------------------------------------------
# AIRFLOW IMPORT & SHIM FALLBACK
# ----------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

    class DAG:
        """Lightweight DAG container for environments without apache-airflow installed."""
        def __init__(
            self,
            dag_id: str,
            default_args: dict | None = None,
            schedule_interval: str | None = None,
            catchup: bool = False,
            tags: list[str] | None = None,
            **kwargs,
        ):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule_interval = schedule_interval
            self.catchup = catchup
            self.tags = tags or []
            self.tasks: dict[str, Any] = {}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class PythonOperator:
        """Lightweight PythonOperator shim to wire dependencies and execute tasks."""
        def __init__(
            self,
            task_id: str,
            python_callable: Any,
            op_kwargs: dict | None = None,
            dag: DAG | None = None,
            **kwargs,
        ):
            self.task_id = task_id
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}
            self.dag = dag
            self.downstream_list: list[Any] = []
            self.upstream_list: list[Any] = []
            if dag is not None and hasattr(dag, "tasks"):
                dag.tasks[task_id] = self

        def __rshift__(self, other: Any):
            """Support the >> dependency operator."""
            if isinstance(other, PythonOperator):
                self.downstream_list.append(other)
                other.upstream_list.append(self)
            return other

        def execute(self, context: Any = None):
            """Execute the underlying python callable."""
            return self.python_callable(**self.op_kwargs)


# ----------------------------------------------------------------------
# PATHS & CONFIGURATION
# ----------------------------------------------------------------------
DEFAULT_FULL_CANDIDATE = PROJECT_ROOT / "data" / "raw" / "full_download" / "acndata_sessions_full_candidate.json"
DEFAULT_SAMPLE_PATH = PROJECT_ROOT / "data" / "sample" / "sample_sessions.json"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def resolve_source_file() -> Path:
    """Resolve the appropriate raw source dataset (full candidate or sample fallback)."""
    if DEFAULT_FULL_CANDIDATE.exists():
        try:
            with open(DEFAULT_FULL_CANDIDATE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("_items") and len(data["_items"]) > 0:
                    return DEFAULT_FULL_CANDIDATE
        except Exception:
            pass
    return DEFAULT_SAMPLE_PATH


# ----------------------------------------------------------------------
# PIPELINE CALLABLE TASKS
# ----------------------------------------------------------------------
def task_extract(**kwargs) -> dict[str, Any]:
    """
    Task 1: Extract raw session records from JSON.
    """
    source_file = resolve_source_file()
    logger.info("DAG Task [extract]: Reading source from %s", source_file)

    sessions, metadata = extract_sessions(str(source_file), allow_recovery=True)
    extraction_meta = metadata.get("_extraction_status", {})
    status = extraction_meta.get("status", "SUCCESS") if isinstance(extraction_meta, dict) else "SUCCESS"

    logger.info("DAG Task [extract]: Extracted %d sessions (status: %s)", len(sessions), status)
    return {
        "status": status,
        "record_count": len(sessions),
        "source_file": str(source_file),
        "sessions": sessions,
        "metadata": metadata,
    }


def task_validate(extract_output: dict[str, Any] | None = None, **kwargs) -> dict[str, Any]:
    """
    Task 2: Validate extracted records against data contracts and flag anomalies.
    """
    if not extract_output or "sessions" not in extract_output:
        extract_output = task_extract()

    sessions = extract_output["sessions"]
    logger.info("DAG Task [validate]: Validating %d session records", len(sessions))

    valid_sessions, summary = validate_sessions(sessions)
    clean_count = summary.get("clean_records", 0)
    flagged_count = summary.get("flagged_records", 0)
    duplicates_count = summary.get("duplicate_records", 0)

    logger.info(
        "DAG Task [validate]: Complete — %d clean, %d flagged, %d duplicates",
        clean_count,
        flagged_count,
        duplicates_count,
    )

    if len(sessions) > 0 and len(valid_sessions) == 0:
        raise ValueError("Validation failed: 0 usable records produced")

    return {
        "valid_sessions": valid_sessions,
        "summary": summary,
        "metadata": extract_output.get("metadata", {}),
    }


def task_transform(validate_output: dict[str, Any] | None = None, **kwargs) -> dict[str, Any]:
    """
    Task 3: Transform and normalize validated sessions into relational DataFrames.
    """
    if not validate_output or "valid_sessions" not in validate_output:
        validate_output = task_validate()

    valid_sessions = validate_output["valid_sessions"]
    logger.info("DAG Task [transform]: Transforming %d sessions", len(valid_sessions))
    sessions_df, user_inputs_df = transform_sessions(valid_sessions)

    logger.info(
        "DAG Task [transform]: Produced %d session rows and %d user-input rows",
        len(sessions_df),
        len(user_inputs_df),
    )

    return {
        "sessions_df": sessions_df,
        "user_inputs_df": user_inputs_df,
        "summary": validate_output.get("summary", {}),
        "metadata": validate_output.get("metadata", {}),
    }


def task_load(transform_output: dict[str, Any] | None = None, **kwargs) -> dict[str, Any]:
    """
    Task 4: Write transformed DataFrames to CSV, JSONL, and quality report files.
    """
    if not transform_output or "sessions_df" not in transform_output:
        transform_output = task_transform()

    sessions_df = transform_output["sessions_df"]
    user_inputs_df = transform_output["user_inputs_df"]
    summary = transform_output.get("summary", {})
    metadata = transform_output.get("metadata", {})

    logger.info("DAG Task [load]: Writing tables to %s", PROCESSED_DIR)
    load_result = load_sessions(
        sessions_df=sessions_df,
        user_inputs_df=user_inputs_df,
        validation_summary=summary,
        metadata=metadata,
        target_dir=str(PROCESSED_DIR),
    )

    return load_result


def task_quality_check(load_output: dict[str, Any] | None = None, **kwargs) -> dict[str, Any]:
    """
    Task 5: Execute post-load verification, assert artifact existence, and foreign key integrity.
    """
    sessions_csv = PROCESSED_DIR / "sessions.csv"
    user_inputs_csv = PROCESSED_DIR / "user_inputs.csv"
    quality_json = PROCESSED_DIR / "quality_report.json"

    assert sessions_csv.exists(), f"Quality check failed: {sessions_csv} does not exist"
    assert user_inputs_csv.exists(), f"Quality check failed: {user_inputs_csv} does not exist"
    assert quality_json.exists(), f"Quality check failed: {quality_json} does not exist"

    import pandas as pd
    s_df = pd.read_csv(sessions_csv)
    u_df = pd.read_csv(user_inputs_csv)

    assert len(s_df) > 0, "Quality check failed: sessions.csv is empty"

    # Foreign key orphan check
    if not u_df.empty:
        valid_sessions = set(s_df["session_id"])
        orphans = u_df[~u_df["session_id"].isin(valid_sessions)]
        assert len(orphans) == 0, f"Quality check failed: {len(orphans)} orphaned user-input records found"

    logger.info("DAG Task [quality_check]: All post-load integrity checks PASSED.")
    return {
        "status": "PASSED",
        "verified_sessions_count": len(s_df),
        "verified_user_inputs_count": len(u_df),
    }


# ----------------------------------------------------------------------
# DAG DEFINITION & WIRING
# ----------------------------------------------------------------------
DAG_ID = "ev_charging_pipeline"
DEFAULT_ARGS = {
    "owner": "shipra",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["ev-charging", "batch-pipeline", "caltech"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        dag=dag,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=task_load,
        dag=dag,
    )

    quality_check_task = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
        dag=dag,
    )

    # Define sequential pipeline dependency graph:
    extract_task >> validate_task >> transform_task >> load_task >> quality_check_task


def run_pipeline_locally() -> dict[str, Any]:
    """
    Convenience runner to execute all DAG tasks sequentially outside of Airflow.
    """
    logger.info("Executing EV Charging Pipeline via DAG local runner...")
    ext = task_extract()
    val = task_validate(ext)
    trn = task_transform(val)
    lod = task_load(trn)
    chk = task_quality_check(lod)
    return {
        "status": "SUCCESS",
        "extract": ext["record_count"],
        "load": lod,
        "quality_check": chk,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    result = run_pipeline_locally()
    print(json.dumps(result, indent=2, default=str))
