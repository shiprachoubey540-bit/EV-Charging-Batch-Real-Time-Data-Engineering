"""
orchestration/run_historical_batch.py — Reproducible orchestrator for historical batch ETL.

Owner: Ananthan S, Aleena Antony, Shipra Choubey

This script coordinates the historical batch ETL workflow:
  1. Input Validation: Ensures raw candidate JSON exists and is intact.
  2. Batch Transformation: Executes PySpark/PyArrow ETL (processing.batch_transform).
  3. Quality Verification: Runs independent read-only auditing (processing.verify_batch).

Guarantees:
  - Deterministic and Idempotent: Reruns safely overwrite analytical outputs; never appends duplicates.
  - Fail-Fast: Any stage failure halts execution immediately and exits with status 1.
  - Dataset Preservation: Protects original data/raw/acndata_sessions.json.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from processing.batch_transform import run_batch_transforms
from processing.verify_batch import verify_historical_batch

logger = logging.getLogger("orchestration.historical_batch")

DEFAULT_CANDIDATE_PATH = PROJECT_ROOT / "data" / "raw" / "full_download" / "acndata_sessions_full_candidate.json"
DEFAULT_HISTORICAL_DIR = PROJECT_ROOT / "data" / "processed" / "historical_batch"
DEFAULT_VALIDATION_DIR = PROJECT_ROOT / "data" / "processed" / "validation"


def execute_historical_workflow(
    candidate_path: Path | str = DEFAULT_CANDIDATE_PATH,
    historical_dir: Path | str = DEFAULT_HISTORICAL_DIR,
    validation_dir: Path | str = DEFAULT_VALIDATION_DIR,
    engine: str = "auto",
) -> dict[str, Any]:
    """
    Execute the end-to-end historical batch ETL workflow.

    Parameters
    ----------
    candidate_path : Path | str
        Path to candidate historical JSON file.
    historical_dir : Path | str
        Output directory for clean Parquet tables and ETL audit report.
    validation_dir : Path | str
        Output directory for verification report and analytical summaries.
    engine : str
        Transformation engine ('auto', 'spark', or 'pandas').

    Returns
    -------
    dict
        Summary of pipeline execution timing and verification status.

    Raises
    ------
    FileNotFoundError
        If candidate dataset is missing.
    RuntimeError
        If transformation or verification fails.
    """
    workflow_start = time.time()
    cand_file = Path(candidate_path).resolve()
    hist_dir = Path(historical_dir).resolve()
    val_dir = Path(validation_dir).resolve()

    logger.info("=" * 70)
    logger.info("STARTING REPRODUCIBLE HISTORICAL BATCH ETL WORKFLOW")
    logger.info("=" * 70)
    logger.info("Input Candidate File: %s", cand_file)
    logger.info("Historical Output:    %s", hist_dir)
    logger.info("Validation Output:    %s", val_dir)
    logger.info("Engine Mode:          %s", engine)

    # 1. Input Check
    if not cand_file.exists():
        err_msg = f"Candidate historical dataset not found at: {cand_file}"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    # 2. Stage 1: Batch Transformation
    logger.info("-" * 70)
    logger.info("STAGE 1/2: BATCH TRANSFORMATION (PySpark / PyArrow)")
    logger.info("-" * 70)
    t0 = time.time()
    try:
        etl_report = run_batch_transforms(
            input_path=cand_file,
            output_dir=hist_dir,
            engine=engine,
        )
    except Exception as exc:
        logger.exception("Batch transformation failed with error: %s", exc)
        raise RuntimeError(f"Stage 1 Batch Transformation failed: {exc}") from exc

    t_etl = time.time() - t0
    logger.info("Stage 1 complete in %.2f seconds (Clean Sessions: %d, User Inputs: %d)",
                t_etl,
                etl_report["record_accounting"]["clean_session_records"],
                etl_report["record_accounting"]["user_input_records"])

    # 3. Stage 2: Verification and Analytics Readiness
    logger.info("-" * 70)
    logger.info("STAGE 2/2: READ-ONLY VERIFICATION & ANALYTICS AUDIT")
    logger.info("-" * 70)
    t0 = time.time()
    try:
        verif_report = verify_historical_batch(
            historical_dir=hist_dir,
            raw_candidate_path=cand_file,
            validation_dir=val_dir,
        )
    except Exception as exc:
        logger.exception("Verification audit failed with error: %s", exc)
        raise RuntimeError(f"Stage 2 Verification failed: {exc}") from exc

    t_verif = time.time() - t0

    if not verif_report.get("all_checks_passed"):
        err_msg = "Verification audit failed one or more data quality checks."
        logger.error("%s Details: %s", err_msg, verif_report)
        raise RuntimeError(err_msg)

    logger.info("Stage 2 complete in %.2f seconds. All quality checks PASSED.", t_verif)

    total_time = time.time() - workflow_start
    summary = {
        "workflow": "historical_batch_etl",
        "status": "SUCCESS",
        "total_elapsed_sec": round(total_time, 2),
        "stage_timings": {
            "transformation_sec": round(t_etl, 2),
            "verification_sec": round(t_verif, 2),
        },
        "records": {
            "input_sessions": etl_report["record_accounting"]["total_input_records"],
            "clean_sessions": etl_report["record_accounting"]["clean_session_records"],
            "user_inputs": etl_report["record_accounting"]["user_input_records"],
            "quarantined_records": etl_report["record_accounting"]["quarantined_records"],
            "active_stations": verif_report["analytics_summary"]["active_stations_count"],
            "total_delivered_kwh": verif_report["analytics_summary"]["total_energy_delivered_kwh"],
        },
        "artifacts": {
            "sessions_parquet": str(hist_dir / "sessions.parquet"),
            "user_inputs_parquet": str(hist_dir / "user_inputs.parquet"),
            "etl_audit_report": str(hist_dir / "etl_audit_report.json"),
            "verification_report": str(val_dir / "verification_report.json"),
            "monthly_summary_csv": str(val_dir / "monthly_sessions_summary.csv"),
            "station_summary_csv": str(val_dir / "station_summary.csv"),
        },
    }

    logger.info("=" * 70)
    logger.info("WORKFLOW COMPLETED SUCCESSFULLY IN %.2f SECONDS", total_time)
    logger.info("=" * 70)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the reproducible historical batch ETL and verification workflow."
    )
    parser.add_argument(
        "--candidate",
        type=str,
        default=str(DEFAULT_CANDIDATE_PATH),
        help="Path to candidate historical JSON file",
    )
    parser.add_argument(
        "--historical-dir",
        type=str,
        default=str(DEFAULT_HISTORICAL_DIR),
        help="Output directory for historical Parquet tables",
    )
    parser.add_argument(
        "--validation-dir",
        type=str,
        default=str(DEFAULT_VALIDATION_DIR),
        help="Output directory for validation artifacts",
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["auto", "spark", "pandas"],
        default="auto",
        help="Transformation engine (default: auto)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    try:
        summary = execute_historical_workflow(
            candidate_path=args.candidate,
            historical_dir=args.historical_dir,
            validation_dir=args.validation_dir,
            engine=args.engine,
        )
        print(json.dumps(summary, indent=2))
        sys.exit(0)
    except Exception as exc:
        logger.error("Historical batch workflow failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
