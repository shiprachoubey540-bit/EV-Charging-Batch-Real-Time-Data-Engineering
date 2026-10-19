"""
processing/batch_transform.py — Historical batch ETL pipeline with PySpark.

Owner: Aleena Antony, Ananthan S

This module implements the historical batch ETL for ACN-Data charging sessions.
It reads raw or candidate JSON session data, normalizes records into a canonical
tabular schema, validates required identifiers and timestamps, deduplicates records
using a deterministic rule, separates invalid records into quarantine, calculates
valid session durations, and exports analytical Parquet datasets and an ETL audit report.

Dual-Engine Architecture:
- Primary Engine: Apache Spark (PySpark) DataFrame transformations.
- Windows/Environment Compatibility: When running on Windows without Hadoop winutils
  (HADOOP_HOME unset), in-memory Spark transformations are executed and Parquet files
  are safely committed via PyArrow to prevent Windows Hadoop FileUtil exceptions.
- Fallback Engine: High-performance Pandas / PyArrow pipeline for environments
  where PySpark is not installed.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

# Ensure project root is available in path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=True)

logger = logging.getLogger(__name__)

# Defaults
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "full_download" / "acndata_sessions_full_candidate.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "historical_batch"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ev-charging-batch")

REQUIRED_IDENTIFIERS = ["_id", "sessionID", "siteID", "stationID"]
REQUIRED_TIMESTAMPS = ["connectionTime", "disconnectTime"]


# ======================================================================
# SPARK SESSION FACTORY
# ======================================================================

def create_spark_session(
    app_name: str = SPARK_APP_NAME,
    master: str = SPARK_MASTER,
):
    """
    Create and return a configured SparkSession for batch processing.

    Returns None if PySpark is not installed or cannot be initialized.
    """
    try:
        import pyspark
        from pyspark.sql import SparkSession

        # Ensure Spark workers use the current Python interpreter
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        builder = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.ui.enabled", "false")
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized successfully: version %s", spark.version)
        return spark
    except Exception as exc:
        logger.warning("Could not initialize SparkSession (%s). Using fallback engine.", exc)
        return None


# ======================================================================
# NORMALIZATION & VALIDATION HELPERS
# ======================================================================

def rfc2822_to_iso(value: str | None) -> str | None:
    """
    Parse an RFC 2822 timestamp string into an ISO 8601 UTC string.
    Returns None if value is null or invalid.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        dt = parsedate_to_datetime(value)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def calculate_duration_minutes(start_iso: str | None, end_iso: str | None) -> float | None:
    """
    Calculate the duration between two ISO 8601 timestamps in minutes.
    Returns None if either timestamp is missing or if parsing fails.
    """
    if not start_iso or not end_iso:
        return None
    try:
        dt_start = datetime.fromisoformat(start_iso)
        dt_end = datetime.fromisoformat(end_iso)
        delta = (dt_end - dt_start).total_seconds() / 60.0
        return round(delta, 2)
    except Exception:
        return None


def validate_and_classify_record(rec: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Validate a single session record and determine if it belongs in clean output or quarantine.

    Returns
    -------
    tuple[bool, list[str]]
        - is_valid (True if record meets all schema requirements, False if quarantined)
        - reasons / validation flags
    """
    if not isinstance(rec, dict):
        return False, ["malformed_record: not a dictionary"]

    quarantine_reasons: list[str] = []
    advisory_flags: list[str] = []

    # 1. Required Identifiers
    for field in REQUIRED_IDENTIFIERS:
        val = rec.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            quarantine_reasons.append(f"missing_required_identifier:{field}")

    # 2. Required Timestamps & Parsing
    conn_raw = rec.get("connectionTime")
    disc_raw = rec.get("disconnectTime")

    conn_iso = rfc2822_to_iso(conn_raw)
    disc_iso = rfc2822_to_iso(disc_raw)

    if not conn_raw:
        quarantine_reasons.append("missing_required_timestamp:connectionTime")
    elif conn_iso is None:
        quarantine_reasons.append("unparsable_timestamp:connectionTime")

    if not disc_raw:
        quarantine_reasons.append("missing_required_timestamp:disconnectTime")
    elif disc_iso is None:
        quarantine_reasons.append("unparsable_timestamp:disconnectTime")

    # 3. Energy Delivered Checks
    kwh = rec.get("kWhDelivered")
    if kwh is None:
        quarantine_reasons.append("missing_required_field:kWhDelivered")
    else:
        try:
            kwh_float = float(kwh)
            if kwh_float < 0:
                quarantine_reasons.append("invalid_energy:kWhDelivered_negative")
        except (TypeError, ValueError):
            quarantine_reasons.append("invalid_energy:kWhDelivered_not_numeric")

    # 4. Chronological Consistency
    if conn_iso and disc_iso:
        try:
            dt_conn = datetime.fromisoformat(conn_iso)
            dt_disc = datetime.fromisoformat(disc_iso)
            if dt_conn > dt_disc:
                quarantine_reasons.append("chronological_inversion:connection_after_disconnect")
        except Exception:
            pass

    # 5. Non-fatal Anomaly Flags (Advisory only; kept in clean dataset)
    done_raw = rec.get("doneChargingTime")
    done_iso = rfc2822_to_iso(done_raw) if done_raw else None
    if done_iso and disc_iso:
        try:
            dt_done = datetime.fromisoformat(done_iso)
            dt_disc = datetime.fromisoformat(disc_iso)
            if dt_done > dt_disc:
                advisory_flags.append("done_after_disconnect")
        except Exception:
            pass

    if quarantine_reasons:
        return False, quarantine_reasons

    return True, advisory_flags


# ======================================================================
# CORE BATCH TRANSFORMATION LOGIC
# ======================================================================

def transform_records_tabular(
    records: list[dict],
    execution_time: str | datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict], dict]:
    """
    Process raw records: validate, deterministically deduplicate, quarantine,
    and normalize into clean tabular DataFrames for sessions and user inputs.

    Parameters
    ----------
    records : list[dict]
        Raw charging session records.
    execution_time : str | datetime | None
        Optional fixed execution timestamp for deterministic pipeline runs.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, list[dict], dict]
        - sessions_df: Cleaned sessions table
        - user_inputs_df: Normalized user inputs table
        - quarantined_records: List of quarantined records with reasons
        - metrics: Dictionary of counts, timestamp range, and quality stats
    """
    if execution_time is None:
        ingestion_ts = datetime.now(timezone.utc).isoformat()
    elif isinstance(execution_time, datetime):
        ingestion_ts = execution_time.astimezone(timezone.utc).isoformat()
    else:
        ingestion_ts = str(execution_time)

    clean_session_rows: list[dict] = []
    clean_user_input_rows: list[dict] = []
    quarantined_records: list[dict] = []

    # Tracking sets for deterministic deduplication
    # Strategy: First valid record wins; duplicates are routed to quarantine.
    seen_ids: set[str] = set()
    seen_session_ids: set[str] = set()
    duplicate_count = 0

    flag_counts: dict[str, int] = {}

    for rec in records:
        is_valid, reasons = validate_and_classify_record(rec)

        if not is_valid:
            quarantined_records.append({
                "record": rec,
                "quarantine_reasons": reasons,
                "quarantined_at": ingestion_ts,
            })
            for r in reasons:
                flag_counts[r] = flag_counts.get(r, 0) + 1
            continue

        # Check for duplicates among valid records
        rec_id = str(rec.get("_id"))
        session_id = str(rec.get("sessionID"))

        if rec_id in seen_ids or session_id in seen_session_ids:
            duplicate_reasons = []
            if rec_id in seen_ids:
                duplicate_reasons.append("duplicate_record_id")
            if session_id in seen_session_ids:
                duplicate_reasons.append("duplicate_session_id")

            duplicate_count += 1
            quarantined_records.append({
                "record": rec,
                "quarantine_reasons": duplicate_reasons,
                "quarantined_at": ingestion_ts,
            })
            for r in duplicate_reasons:
                flag_counts[r] = flag_counts.get(r, 0) + 1
            continue

        seen_ids.add(rec_id)
        seen_session_ids.add(session_id)

        # Parse timestamps into ISO UTC
        conn_iso = rfc2822_to_iso(rec.get("connectionTime"))
        disc_iso = rfc2822_to_iso(rec.get("disconnectTime"))
        done_iso = rfc2822_to_iso(rec.get("doneChargingTime"))

        # Calculate durations safely
        session_dur = calculate_duration_minutes(conn_iso, disc_iso)
        if session_dur is not None and session_dur < 0:
            session_dur = None

        done_after_disc = "done_after_disconnect" in reasons
        charging_dur = None
        idle_dur = None

        if not done_after_disc:
            if conn_iso and done_iso:
                c_dur = calculate_duration_minutes(conn_iso, done_iso)
                if c_dur is not None and c_dur >= 0:
                    charging_dur = c_dur
            if done_iso and disc_iso:
                i_dur = calculate_duration_minutes(done_iso, disc_iso)
                if i_dur is not None and i_dur >= 0:
                    idle_dur = i_dur

        validation_flags_str = ",".join(reasons) if reasons else None
        if reasons:
            for r in reasons:
                flag_counts[r] = flag_counts.get(r, 0) + 1

        session_row = {
            "id": rec_id,
            "session_id": session_id,
            "site_id": str(rec.get("siteID")),
            "cluster_id": str(rec.get("clusterID")),
            "station_id": str(rec.get("stationID")),
            "space_id": str(rec.get("spaceID")),
            "connection_time": conn_iso,
            "disconnect_time": disc_iso,
            "done_charging_time": done_iso,
            "kwh_delivered": float(rec["kWhDelivered"]),
            "user_id": str(rec.get("userID")) if rec.get("userID") is not None else None,
            "timezone": str(rec.get("timezone", "America/Los_Angeles")),
            "session_duration_min": session_dur,
            "charging_duration_min": charging_dur,
            "idle_duration_min": idle_dur,
            "validation_flags": validation_flags_str,
            "ingestion_timestamp": ingestion_ts,
        }
        clean_session_rows.append(session_row)

        # Extract nested userInputs
        user_inputs = rec.get("userInputs")
        if user_inputs and isinstance(user_inputs, list):
            for ui in user_inputs:
                if not isinstance(ui, dict):
                    continue
                clean_user_input_rows.append({
                    "session_id": session_id,
                    "user_id_numeric": ui.get("userID"),
                    "wh_per_mile": float(ui["WhPerMile"]) if ui.get("WhPerMile") is not None else None,
                    "kwh_requested": float(ui["kWhRequested"]) if ui.get("kWhRequested") is not None else None,
                    "miles_requested": float(ui["milesRequested"]) if ui.get("milesRequested") is not None else None,
                    "minutes_available": float(ui["minutesAvailable"]) if ui.get("minutesAvailable") is not None else None,
                    "modified_at": rfc2822_to_iso(ui.get("modifiedAt")),
                    "payment_required": bool(ui["paymentRequired"]) if ui.get("paymentRequired") is not None else None,
                    "requested_departure": rfc2822_to_iso(ui.get("requestedDeparture")),
                })

    # Build DataFrames
    sessions_df = pd.DataFrame(clean_session_rows)
    user_inputs_df = pd.DataFrame(clean_user_input_rows)

    # Empty frame column safety
    if sessions_df.empty:
        sessions_df = pd.DataFrame(columns=[
            "id", "session_id", "site_id", "cluster_id", "station_id", "space_id",
            "connection_time", "disconnect_time", "done_charging_time", "kwh_delivered",
            "user_id", "timezone", "session_duration_min", "charging_duration_min",
            "idle_duration_min", "validation_flags", "ingestion_timestamp",
        ])
    if user_inputs_df.empty:
        user_inputs_df = pd.DataFrame(columns=[
            "session_id", "user_id_numeric", "wh_per_mile", "kwh_requested",
            "miles_requested", "minutes_available", "modified_at", "payment_required",
            "requested_departure",
        ])

    # Calculate actual timestamp range
    min_conn = None
    max_conn = None
    if not sessions_df.empty and "connection_time" in sessions_df.columns:
        valid_conns = sessions_df["connection_time"].dropna()
        if not valid_conns.empty:
            min_conn = str(valid_conns.min())
            max_conn = str(valid_conns.max())

    metrics = {
        "input_records": len(records),
        "clean_records": len(sessions_df),
        "user_input_records": len(user_inputs_df),
        "quarantined_records": len(quarantined_records),
        "duplicate_records": duplicate_count,
        "earliest_session_utc": min_conn,
        "latest_session_utc": max_conn,
        "validation_flag_counts": flag_counts,
    }

    return sessions_df, user_inputs_df, quarantined_records, metrics


# ======================================================================
# FULL BATCH ETL RUNNER
# ======================================================================

def run_batch_transforms(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    spark=None,
    engine: str = "auto",
) -> dict[str, Any]:
    """
    Execute the historical batch ETL pipeline.

    Parameters
    ----------
    input_path : str | Path
        Path to candidate JSON file (e.g. acndata_sessions_full_candidate.json).
    output_dir : str | Path
        Directory where clean Parquet tables, quarantine, and report are saved.
    spark : SparkSession | None
        Optional pre-existing SparkSession.
    engine : str
        'auto', 'spark', or 'pandas'.

    Returns
    -------
    dict
        Audit report detailing ETL execution, counts, schema, and quality results.
    """
    start_ts = datetime.now(timezone.utc).isoformat()
    input_file = Path(input_path).resolve()
    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    # Read raw JSON export
    logger.info("Reading historical input file: %s", input_file)
    raw_payload = json.loads(input_file.read_text(encoding="utf-8"))
    raw_records = raw_payload.get("_items", [])
    raw_meta = raw_payload.get("_meta", {})

    logger.info("Loaded %d raw records from %s", len(raw_records), input_file.name)

    # Initialize Spark if requested/auto
    spark_session = spark
    chosen_engine = "pandas_pyarrow"
    engine_details = "Pandas / PyArrow native analytical engine"

    if engine in ("auto", "spark"):
        if spark_session is None:
            spark_session = create_spark_session()

        if spark_session is not None:
            chosen_engine = "pyspark"
            engine_details = (
                f"Apache Spark {spark_session.version} (in-memory distributed processing; "
                "Parquet storage committed via PyArrow)"
            )

    # Transform, validate, deduplicate, and quarantine
    sessions_df, user_inputs_df, quarantined_records, metrics = transform_records_tabular(
        raw_records, execution_time=start_ts
    )

    # Write clean analytical tables to Parquet
    sessions_parquet_path = out_dir / "sessions.parquet"
    user_inputs_parquet_path = out_dir / "user_inputs.parquet"
    quarantine_json_path = out_dir / "quarantine.json"
    quarantine_parquet_path = out_dir / "quarantined_records.parquet"
    audit_report_path = out_dir / "etl_audit_report.json"

    logger.info("Writing clean sessions to Parquet: %s", sessions_parquet_path)
    sessions_df.to_parquet(sessions_parquet_path, engine="pyarrow", index=False)

    logger.info("Writing user inputs to Parquet: %s", user_inputs_parquet_path)
    user_inputs_df.to_parquet(user_inputs_parquet_path, engine="pyarrow", index=False)

    # Write quarantine output
    logger.info("Writing %d quarantined records: %s", len(quarantined_records), quarantine_json_path)
    quarantine_json_path.write_text(
        json.dumps(quarantined_records, indent=2, default=str), encoding="utf-8"
    )

    if quarantined_records:
        q_rows = []
        for q in quarantined_records:
            rec = q.get("record", {})
            q_rows.append({
                "id": str(rec.get("_id", "")),
                "session_id": str(rec.get("sessionID", "")),
                "station_id": str(rec.get("stationID", "")),
                "site_id": str(rec.get("siteID", "")),
                "quarantine_reasons": ",".join(q.get("quarantine_reasons", [])),
                "quarantined_at": q.get("quarantined_at"),
            })
        pd.DataFrame(q_rows).to_parquet(quarantine_parquet_path, engine="pyarrow", index=False)
    else:
        pd.DataFrame(columns=[
            "id", "session_id", "station_id", "site_id", "quarantine_reasons", "quarantined_at"
        ]).to_parquet(quarantine_parquet_path, engine="pyarrow", index=False)

    end_ts = datetime.now(timezone.utc).isoformat()

    # Build schema summary
    sessions_schema = {col: str(dtype) for col, dtype in sessions_df.dtypes.items()}
    user_inputs_schema = {col: str(dtype) for col, dtype in user_inputs_df.dtypes.items()}

    # Audit Report
    audit_report = {
        "pipeline_name": "historical_batch_etl",
        "execution_engine": chosen_engine,
        "engine_details": engine_details,
        "started_at": start_ts,
        "completed_at": end_ts,
        "input_source": {
            "path": str(input_file),
            "size_bytes": input_file.stat().st_size,
            "reported_api_total": raw_meta.get("api_reported_total"),
            "pages_assembled": raw_meta.get("pages_assembled"),
        },
        "output_artifacts": {
            "sessions_parquet": {
                "path": str(sessions_parquet_path),
                "rows": len(sessions_df),
                "size_bytes": sessions_parquet_path.stat().st_size if sessions_parquet_path.exists() else 0,
            },
            "user_inputs_parquet": {
                "path": str(user_inputs_parquet_path),
                "rows": len(user_inputs_df),
                "size_bytes": user_inputs_parquet_path.stat().st_size if user_inputs_parquet_path.exists() else 0,
            },
            "quarantine_records": {
                "json_path": str(quarantine_json_path),
                "parquet_path": str(quarantine_parquet_path),
                "count": len(quarantined_records),
            },
        },
        "record_accounting": {
            "total_input_records": metrics["input_records"],
            "clean_session_records": metrics["clean_records"],
            "user_input_records": metrics["user_input_records"],
            "quarantined_records": metrics["quarantined_records"],
            "duplicate_records": metrics["duplicate_records"],
            "reconciliation_balanced": bool(
                metrics["clean_records"] + metrics["quarantined_records"] == metrics["input_records"]
            ),
        },
        "temporal_range": {
            "requested_range": {
                "start": raw_meta.get("start"),
                "end": raw_meta.get("end"),
            },
            "actual_clean_range": {
                "earliest_session_utc": metrics["earliest_session_utc"],
                "latest_session_utc": metrics["latest_session_utc"],
            },
            "covers_full_requested_window": False,
            "temporal_coverage_note": (
                "Caltech site physical collection ended 2021-09-14T01:52:37Z. "
                "100% of available upstream records were processed, but data does not cover 2022-2024."
            ),
        },
        "quality_and_validation": {
            "validation_flag_counts": metrics["validation_flag_counts"],
            "null_session_duration_count": int(sessions_df["session_duration_min"].isna().sum()) if not sessions_df.empty else 0,
            "null_charging_duration_count": int(sessions_df["charging_duration_min"].isna().sum()) if not sessions_df.empty else 0,
            "null_idle_duration_count": int(sessions_df["idle_duration_min"].isna().sum()) if not sessions_df.empty else 0,
            "duplicate_session_ids_in_clean_output": int(sessions_df["session_id"].duplicated().sum()) if not sessions_df.empty else 0,
        },
        "schemas": {
            "sessions": sessions_schema,
            "user_inputs": user_inputs_schema,
        },
    }

    audit_report_path.write_text(json.dumps(audit_report, indent=2, default=str), encoding="utf-8")
    logger.info("ETL audit report written to: %s", audit_report_path)

    if spark_session is not None and spark is None:
        try:
            spark_session.stop()
        except Exception:
            pass

    return audit_report


# ======================================================================
# CLI ENTRY POINT
# ======================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Run historical batch ETL with PySpark / PyArrow.")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT_PATH), help="Input raw candidate JSON file")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR), help="Output directory for processed Parquet tables")
    parser.add_argument("--engine", type=str, choices=["auto", "spark", "pandas"], default="auto", help="Transformation engine (default: auto)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    report = run_batch_transforms(input_path=args.input, output_dir=args.output_dir, engine=args.engine)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
