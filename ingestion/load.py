"""
ingestion/load.py — Write cleaned session data to the processed directory.

Owner: Ananthan S

This module writes the transformed DataFrames to ``data/processed/`` as
CSV and JSON Lines files, along with a quality report.  It does **not**
write directly to PostgreSQL — that responsibility belongs to the
database module.
"""

import csv
import json
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATA_PROCESSED_DIR = os.getenv("DATA_PROCESSED_DIR", "./data/processed")


def load_sessions(
    sessions_df: pd.DataFrame,
    user_inputs_df: pd.DataFrame,
    validation_summary: dict,
    metadata: dict,
    target_dir: str | None = None,
) -> dict:
    """
    Write transformed data to the processed directory.

    Outputs
    -------
    - ``sessions.csv`` — Main sessions table (quoted non-numerics to preserve string IDs).
    - ``sessions.jsonl`` — Main sessions in JSON Lines format (Spark-friendly).
    - ``user_inputs.csv`` — Normalised user-input entries.
    - ``quality_report.json`` — Validation summary and pipeline metadata.

    Parameters
    ----------
    sessions_df : pd.DataFrame
        Transformed sessions.
    user_inputs_df : pd.DataFrame
        Normalised user inputs.
    validation_summary : dict
        Summary from the validation step.
    metadata : dict
        The ``_meta`` dict from the source file.
    target_dir : str | None
        Override output directory. Defaults to ``data/processed/``.

    Returns
    -------
    dict
        A summary of what was written (paths and row counts).
    """
    out_dir = Path(target_dir or DATA_PROCESSED_DIR).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    results: dict = {"output_dir": str(out_dir), "files": {}}

    # --- Sessions CSV ---
    # Using QUOTE_NONNUMERIC ensures string columns like site_id ("0002")
    # and user_id ("000001083") preserve their leading zeros in CSV roundtrips.
    sessions_csv = out_dir / "sessions.csv"
    sessions_df.to_csv(
        sessions_csv,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_NONNUMERIC,
    )
    results["files"]["sessions.csv"] = {
        "path": str(sessions_csv),
        "rows": len(sessions_df),
    }
    logger.info("Load: wrote %d rows to %s", len(sessions_df), sessions_csv)

    # --- Sessions JSON Lines ---
    sessions_jsonl = out_dir / "sessions.jsonl"
    sessions_df.to_json(
        sessions_jsonl,
        orient="records",
        lines=True,
        date_format="iso",
        force_ascii=False,
    )
    results["files"]["sessions.jsonl"] = {
        "path": str(sessions_jsonl),
        "rows": len(sessions_df),
    }
    logger.info("Load: wrote %d rows to %s", len(sessions_df), sessions_jsonl)

    # --- User Inputs CSV ---
    user_inputs_csv = out_dir / "user_inputs.csv"
    user_inputs_df.to_csv(
        user_inputs_csv,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_NONNUMERIC,
    )
    results["files"]["user_inputs.csv"] = {
        "path": str(user_inputs_csv),
        "rows": len(user_inputs_df),
    }
    logger.info("Load: wrote %d rows to %s", len(user_inputs_df), user_inputs_csv)

    # --- Quality Report ---
    extraction_status = metadata.get("_extraction_status", {})
    clean_metadata = {k: v for k, v in metadata.items() if not k.startswith("_")}

    quality_report = {
        "extraction": extraction_status,
        "source_metadata": clean_metadata,
        "validation": validation_summary,
        "output": results,
    }
    report_path = out_dir / "quality_report.json"
    report_path.write_text(
        json.dumps(quality_report, indent=2, default=str),
        encoding="utf-8",
    )
    results["files"]["quality_report.json"] = {"path": str(report_path)}
    logger.info("Load: wrote quality report to %s", report_path)

    return results


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion.extract import extract_sessions
    from ingestion.validate import validate_sessions
    from ingestion.transform import transform_sessions

    raw, meta = extract_sessions()
    validated, summary = validate_sessions(raw)
    sess_df, ui_df = transform_sessions(validated)
    results = load_sessions(sess_df, ui_df, summary, meta)
    print(json.dumps(results, indent=2))
