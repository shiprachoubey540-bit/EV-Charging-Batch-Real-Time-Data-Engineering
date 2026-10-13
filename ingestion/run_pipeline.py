"""
ingestion/run_pipeline.py — ETL pipeline orchestrator.

Owner: Ananthan S

Wires together Extract → Validate → Transform → Load and provides
structured logging, timing, and error handling.

Usage
-----
    python -m ingestion.run_pipeline
    python -m ingestion.run_pipeline --source data/raw/acndata_sessions.json
    python -m ingestion.run_pipeline --source data/sample/sample_sessions.json
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from ingestion.extract import extract_sessions
from ingestion.validate import validate_sessions
from ingestion.transform import transform_sessions
from ingestion.load import load_sessions

logger = logging.getLogger("ingestion")


def _setup_logging(log_dir: Path) -> None:
    """Configure console + file logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger("ingestion")
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def run_pipeline(
    source_path: str | None = None,
    output_dir: str | None = None,
) -> dict:
    """
    Run the full ETL pipeline.

    Parameters
    ----------
    source_path : str | None
        Path to the source JSON file.
    output_dir : str | None
        Override output directory (defaults to ``data/processed/``).

    Returns
    -------
    dict
        Pipeline results including timing and output summary.
    """
    pipeline_start = time.time()
    results: dict = {"stages": {}}

    # ---- EXTRACT ----
    logger.info("=" * 60)
    logger.info("STAGE 1/4: EXTRACT")
    logger.info("=" * 60)
    t0 = time.time()
    sessions, metadata = extract_sessions(source_path)
    results["stages"]["extract"] = {
        "records": len(sessions),
        "elapsed_sec": round(time.time() - t0, 3),
    }

    # ---- VALIDATE ----
    logger.info("=" * 60)
    logger.info("STAGE 2/4: VALIDATE")
    logger.info("=" * 60)
    t0 = time.time()
    validated, validation_summary = validate_sessions(sessions)
    results["stages"]["validate"] = {
        "summary": validation_summary,
        "elapsed_sec": round(time.time() - t0, 3),
    }

    # ---- TRANSFORM ----
    logger.info("=" * 60)
    logger.info("STAGE 3/4: TRANSFORM")
    logger.info("=" * 60)
    t0 = time.time()
    sessions_df, user_inputs_df = transform_sessions(validated)
    results["stages"]["transform"] = {
        "session_rows": len(sessions_df),
        "user_input_rows": len(user_inputs_df),
        "elapsed_sec": round(time.time() - t0, 3),
    }

    # ---- LOAD ----
    logger.info("=" * 60)
    logger.info("STAGE 4/4: LOAD")
    logger.info("=" * 60)
    t0 = time.time()
    load_results = load_sessions(
        sessions_df, user_inputs_df, validation_summary, metadata, output_dir
    )
    results["stages"]["load"] = {
        "output": load_results,
        "elapsed_sec": round(time.time() - t0, 3),
    }

    # ---- SUMMARY ----
    total_elapsed = round(time.time() - pipeline_start, 3)
    results["total_elapsed_sec"] = total_elapsed

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info("Total records:      %d", len(sessions_df))
    logger.info("User input records: %d", len(user_inputs_df))
    logger.info("Flagged records:    %d", validation_summary.get("flagged_records", 0))
    logger.info("Total time:         %.3f sec", total_elapsed)
    logger.info("Output directory:   %s", load_results.get("output_dir", "N/A"))

    return results


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run the EV Charging Sessions ETL pipeline."
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Path to the source JSON file (default: data/raw/acndata_sessions.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: data/processed/)",
    )
    args = parser.parse_args()

    output_dir = args.output_dir or "./data/processed"
    _setup_logging(Path(output_dir))

    try:
        results = run_pipeline(args.source, output_dir)
        logger.info("Pipeline results:\n%s", json.dumps(results, indent=2, default=str))
    except Exception:
        logger.exception("Pipeline failed with an unhandled error")
        sys.exit(1)


if __name__ == "__main__":
    main()
