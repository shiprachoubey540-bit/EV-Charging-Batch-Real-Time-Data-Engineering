"""
tests/test_orchestration.py — Automated test suite for Airflow DAG & Orchestration.

Owner: Shipra Choubey
"""

from pathlib import Path
import pytest

from orchestration.dags.ev_pipeline_dag import (
    dag,
    DAG_ID,
    DEFAULT_ARGS,
    task_extract,
    task_validate,
    task_transform,
    task_load,
    task_quality_check,
    run_pipeline_locally,
)


def test_dag_configuration():
    """Verify DAG identifier, default arguments, and schedule configuration."""
    assert DAG_ID == "ev_charging_pipeline"
    assert dag.dag_id == "ev_charging_pipeline"
    assert DEFAULT_ARGS["owner"] == "shipra"
    assert DEFAULT_ARGS["retries"] >= 1
    assert dag.schedule_interval == "@daily"


def test_dag_task_graph_dependencies():
    """Verify that all 5 tasks exist and are wired in strict sequential order."""
    expected_tasks = {"extract", "validate", "transform", "load", "quality_check"}
    actual_tasks = set(dag.tasks.keys())
    assert expected_tasks.issubset(actual_tasks)

    # Validate dependency order: extract >> validate >> transform >> load >> quality_check
    t_extract = dag.tasks["extract"]
    t_validate = dag.tasks["validate"]
    t_transform = dag.tasks["transform"]
    t_load = dag.tasks["load"]
    t_quality = dag.tasks["quality_check"]

    assert t_validate in t_extract.downstream_list
    assert t_transform in t_validate.downstream_list
    assert t_load in t_transform.downstream_list
    assert t_quality in t_load.downstream_list


def test_individual_task_callables():
    """Verify that each task callable can execute and return expected data contracts."""
    ext = task_extract()
    assert ext["record_count"] > 0
    assert "sessions" in ext

    val = task_validate(ext)
    assert len(val["valid_sessions"]) > 0
    assert "summary" in val

    trn = task_transform(val)
    assert len(trn["sessions_df"]) > 0
    assert len(trn["user_inputs_df"]) > 0

    lod = task_load(trn)
    assert "files" in lod

    chk = task_quality_check(lod)
    assert chk["status"] == "PASSED"
    assert chk["verified_sessions_count"] > 0


def test_end_to_end_local_orchestration():
    """Execute complete orchestrated workflow end-to-end and assert successful completion."""
    result = run_pipeline_locally()
    assert result["status"] == "SUCCESS"
    assert result["extract"] > 0
    assert result["quality_check"]["status"] == "PASSED"
