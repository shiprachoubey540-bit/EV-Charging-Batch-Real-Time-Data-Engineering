"""
processing/verify_batch.py — Batch verification, staging assertions & analytics readiness.

Owner: Aleena Antony, Ananthan S

Performs independent read-only verification on:
  - data/processed/historical_batch/sessions.parquet (or .csv)
  - data/processed/historical_batch/user_inputs.parquet (or .csv)
  - data/processed/historical_batch/etl_audit_report.json
  - data/raw/full_download/acndata_sessions_full_candidate.json (or data/sample/sample_sessions.json)

Staging & Quality Assertions:
  1. Row counts & mathematical reconciliation against raw input and audit report.
  2. Schema validity: adheres to contracts/data_contracts.yaml canonical fields.
  3. Null constraints: zero null values on required primary and business keys.
  4. Identifier uniqueness: zero duplicate session_id or id.
  5. Referential integrity: all user_inputs.session_id exist in sessions.session_id.
  6. Domain constraints: strictly non-negative energy and duration; chronological ordering.
  7. Anomaly isolation: done_after_disconnect anomalies have nulled duration metrics.
  8. Analytical readiness tables: monthly, daily, station, and hourly peak aggregations.

Outputs saved to: data/processed/validation/
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

DEFAULT_HISTORICAL_DIR = PROJECT_ROOT / "data" / "processed" / "historical_batch"
DEFAULT_RAW_CANDIDATE = PROJECT_ROOT / "data" / "raw" / "full_download" / "acndata_sessions_full_candidate.json"
DEFAULT_SAMPLE_PATH = PROJECT_ROOT / "data" / "sample" / "sample_sessions.json"
DEFAULT_VALIDATION_DIR = PROJECT_ROOT / "data" / "processed" / "validation"

REQUIRED_SESSION_COLUMNS = [
    "id", "session_id", "site_id", "cluster_id", "station_id", "space_id",
    "connection_time", "disconnect_time", "done_charging_time", "kwh_delivered",
    "user_id", "timezone", "session_duration_min", "charging_duration_min",
    "idle_duration_min", "validation_flags", "ingestion_timestamp",
]

REQUIRED_USER_INPUT_COLUMNS = [
    "session_id", "user_id_numeric", "wh_per_mile", "kwh_requested",
    "miles_requested", "minutes_available", "modified_at", "payment_required",
    "requested_departure",
]

NON_NULLABLE_SESSION_COLUMNS = [
    "id", "session_id", "site_id", "cluster_id", "station_id", "space_id",
    "connection_time", "disconnect_time", "kwh_delivered", "timezone",
    "ingestion_timestamp",
]


def read_dataframe_safe(path: Path) -> pd.DataFrame:
    """
    Read tabular data from Parquet, or fallback to CSV sibling if pyarrow is unavailable.
    """
    path = Path(path).resolve()
    # Attempt Parquet first
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            pass

    # Sibling CSV fallback
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)

    if path.exists():
        return pd.read_parquet(path)

    raise FileNotFoundError(f"File not found: {path} (or {csv_path})")


def assert_staging_readiness(
    sessions_df: pd.DataFrame,
    user_inputs_df: pd.DataFrame,
    audit_report: dict[str, Any],
) -> dict[str, Any]:
    """
    Assert row counts, schema validity, null constraints, uniqueness,
    referential integrity, and non-negativity before writing results
    to PostgreSQL or staging Parquet tables.

    Raises
    ------
    AssertionError
        If any critical data contract or data quality rule is violated.
    """
    errors: list[str] = []

    # 1. Row counts & reconciliation
    rec_acc = audit_report.get("record_accounting", {})
    expected_sessions = rec_acc.get("clean_session_records", len(sessions_df))
    expected_uis = rec_acc.get("user_input_records", len(user_inputs_df))

    if len(sessions_df) != expected_sessions:
        errors.append(f"Row count mismatch: sessions_df has {len(sessions_df)}, expected {expected_sessions}")
    if len(user_inputs_df) != expected_uis:
        errors.append(f"Row count mismatch: user_inputs_df has {len(user_inputs_df)}, expected {expected_uis}")

    # 2. Schema validity
    missing_session_cols = [c for c in REQUIRED_SESSION_COLUMNS if c not in sessions_df.columns]
    if missing_session_cols:
        errors.append(f"Missing required session columns: {missing_session_cols}")

    missing_ui_cols = [c for c in REQUIRED_USER_INPUT_COLUMNS if c not in user_inputs_df.columns]
    if missing_ui_cols:
        errors.append(f"Missing required user_inputs columns: {missing_ui_cols}")

    # 3. Null constraints on primary non-nullable fields
    for col in NON_NULLABLE_SESSION_COLUMNS:
        if col in sessions_df.columns:
            null_count = int(sessions_df[col].isna().sum())
            if null_count > 0:
                errors.append(f"Null constraint violated: column '{col}' has {null_count} nulls")

    # 4. Primary key & identifier uniqueness
    if "session_id" in sessions_df.columns:
        dup_session_ids = int(sessions_df["session_id"].duplicated().sum())
        if dup_session_ids > 0:
            errors.append(f"Duplicate session_id detected: {dup_session_ids} duplicates")

    if "id" in sessions_df.columns:
        dup_ids = int(sessions_df["id"].duplicated().sum())
        if dup_ids > 0:
            errors.append(f"Duplicate id detected: {dup_ids} duplicates")

    # 5. Referential integrity
    if "session_id" in sessions_df.columns and "session_id" in user_inputs_df.columns:
        session_id_set = set(sessions_df["session_id"].dropna())
        ui_session_id_set = set(user_inputs_df["session_id"].dropna())
        orphans = ui_session_id_set - session_id_set
        if orphans:
            errors.append(f"Referential integrity failure: {len(orphans)} orphaned session_ids in user_inputs")

    # 6. Non-negative constraints
    if "kwh_delivered" in sessions_df.columns:
        neg_kwh = int((sessions_df["kwh_delivered"] < 0).sum())
        if neg_kwh > 0:
            errors.append(f"Non-negative constraint violated: {neg_kwh} sessions have kwh_delivered < 0")

    if "session_duration_min" in sessions_df.columns:
        neg_dur = int((sessions_df["session_duration_min"].dropna() < 0).sum())
        if neg_dur > 0:
            errors.append(f"Non-negative constraint violated: {neg_dur} sessions have session_duration_min < 0")

    # 7. Anomaly isolation: done_after_disconnect duration metrics must be null
    if "done_charging_time" in sessions_df.columns and "disconnect_time" in sessions_df.columns:
        anom_mask = (
            sessions_df["done_charging_time"].notna()
            & sessions_df["disconnect_time"].notna()
            & (sessions_df["done_charging_time"] > sessions_df["disconnect_time"])
        )
        if anom_mask.any():
            anom_df = sessions_df[anom_mask]
            bad_charging = int(anom_df["charging_duration_min"].notna().sum())
            bad_idle = int(anom_df["idle_duration_min"].notna().sum())
            if bad_charging > 0 or bad_idle > 0:
                errors.append(f"Anomaly isolation failure: {bad_charging} charging / {bad_idle} idle durations not nulled for done_after_disconnect")

    if errors:
        msg = f"Staging readiness assertions failed with {len(errors)} error(s):\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(msg)
        raise AssertionError(msg)

    logger.info("All staging readiness assertions passed successfully.")
    return {
        "staging_readiness": "PASSED",
        "clean_sessions": len(sessions_df),
        "clean_user_inputs": len(user_inputs_df),
        "checked_constraints": [
            "row_counts_reconciliation",
            "schema_contract_validity",
            "null_constraints",
            "identifier_uniqueness",
            "referential_integrity",
            "non_negativity",
            "anomaly_isolation",
        ],
    }


def verify_historical_batch(
    historical_dir: Path | str = DEFAULT_HISTORICAL_DIR,
    raw_candidate_path: Path | str = DEFAULT_RAW_CANDIDATE,
    validation_dir: Path | str = DEFAULT_VALIDATION_DIR,
) -> dict[str, Any]:
    """
    Perform read-only verification of historical batch outputs and compute analytics readiness tables.
    """
    hist_dir = Path(historical_dir).resolve()
    raw_path = Path(raw_candidate_path).resolve()
    val_dir = Path(validation_dir).resolve()
    val_dir.mkdir(parents=True, exist_ok=True)

    sessions_path = hist_dir / "sessions.parquet"
    user_inputs_path = hist_dir / "user_inputs.parquet"
    audit_report_path = hist_dir / "etl_audit_report.json"

    # Fallback to sample data if candidate file is not present
    if not raw_path.exists() and DEFAULT_SAMPLE_PATH.exists() and "full_candidate" in str(raw_path):
        logger.warning("Raw candidate not found at %s. Falling back to sample data: %s", raw_path, DEFAULT_SAMPLE_PATH)
        raw_path = DEFAULT_SAMPLE_PATH

    if not sessions_path.exists() and not (hist_dir / "sessions.csv").exists():
        raise FileNotFoundError(f"Missing sessions data in {hist_dir}")
    if not user_inputs_path.exists() and not (hist_dir / "user_inputs.csv").exists():
        raise FileNotFoundError(f"Missing user inputs data in {hist_dir}")
    if not audit_report_path.exists():
        raise FileNotFoundError(f"Missing audit report: {audit_report_path}")
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw candidate JSON: {raw_path}")

    logger.info("Reading processed files from %s", hist_dir)
    sessions_df = read_dataframe_safe(sessions_path)
    user_inputs_df = read_dataframe_safe(user_inputs_path)
    audit_report = json.loads(audit_report_path.read_text(encoding="utf-8"))

    logger.info("Reading raw candidate dataset from %s", raw_path)
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    if isinstance(raw_payload, dict):
        raw_records = raw_payload.get("_items", [])
    elif isinstance(raw_payload, list):
        raw_records = raw_payload
    else:
        raw_records = []

    raw_uis_count = sum(
        len(item.get("userInputs", []))
        for item in raw_records
        if isinstance(item.get("userInputs"), list)
    )

    # ------------------------------------------------------------------
    # 0. Assert Staging Readiness
    # ------------------------------------------------------------------
    staging_res = assert_staging_readiness(sessions_df, user_inputs_df, audit_report)

    # ------------------------------------------------------------------
    # 1. Row counts & Reconciliation
    # ------------------------------------------------------------------
    expected_sessions = audit_report["record_accounting"]["clean_session_records"]
    expected_uis = audit_report["record_accounting"]["user_input_records"]
    expected_quarantined = audit_report["record_accounting"]["quarantined_records"]
    raw_input_count = len(raw_records)

    reconciliation_passed = (
        len(sessions_df) == expected_sessions
        and len(user_inputs_df) == expected_uis
        and raw_input_count == (len(sessions_df) + expected_quarantined)
        and raw_uis_count == len(user_inputs_df)
    )

    # ------------------------------------------------------------------
    # 2. Identifier Uniqueness
    # ------------------------------------------------------------------
    unique_session_ids = int(sessions_df["session_id"].nunique())
    unique_ids = int(sessions_df["id"].nunique())
    duplicate_session_ids = int(sessions_df["session_id"].duplicated().sum())
    duplicate_ids = int(sessions_df["id"].duplicated().sum())
    uniqueness_passed = (duplicate_session_ids == 0 and duplicate_ids == 0)

    # ------------------------------------------------------------------
    # 3. Referential Integrity
    # ------------------------------------------------------------------
    session_id_set = set(sessions_df["session_id"])
    ui_session_id_set = set(user_inputs_df["session_id"])
    orphaned_uis = ui_session_id_set - session_id_set
    referential_integrity_passed = len(orphaned_uis) == 0

    # ------------------------------------------------------------------
    # 4. UTC Timestamp Formatting & Consistency
    # ------------------------------------------------------------------
    ts_cols = ["connection_time", "disconnect_time", "done_charging_time"]
    non_utc_counts = {}
    for col in ts_cols:
        if col in sessions_df.columns:
            s_non_null = sessions_df[col].dropna().astype(str)
            non_utc = (~s_non_null.str.endswith("+00:00") & ~s_non_null.str.endswith("Z")).sum()
            non_utc_counts[col] = int(non_utc)
    utc_consistency_passed = all(count == 0 for count in non_utc_counts.values())

    # ------------------------------------------------------------------
    # 5. Non-Negative Validations
    # ------------------------------------------------------------------
    negative_session_dur = int((sessions_df["session_duration_min"] < 0).sum())
    negative_charging_dur = int((sessions_df["charging_duration_min"].dropna() < 0).sum())
    negative_idle_dur = int((sessions_df["idle_duration_min"].dropna() < 0).sum())
    negative_kwh = int((sessions_df["kwh_delivered"] < 0).sum())
    non_negative_passed = (
        negative_session_dur == 0
        and negative_charging_dur == 0
        and negative_idle_dur == 0
        and negative_kwh == 0
    )

    # ------------------------------------------------------------------
    # 6. done_after_disconnect Anomaly Recalculation
    # ------------------------------------------------------------------
    done_after_disc_mask = (
        sessions_df["done_charging_time"].notna()
        & sessions_df["disconnect_time"].notna()
        & (sessions_df["done_charging_time"] > sessions_df["disconnect_time"])
    )
    done_after_disc_count = int(done_after_disc_mask.sum())
    anomalous_sessions = sessions_df[done_after_disc_mask]

    non_null_charging_in_anomalies = int(anomalous_sessions["charging_duration_min"].notna().sum())
    non_null_idle_in_anomalies = int(anomalous_sessions["idle_duration_min"].notna().sum())
    all_flagged = bool((anomalous_sessions["validation_flags"] == "done_after_disconnect").all()) if done_after_disc_count > 0 else True

    expected_anomalies = audit_report.get("quality_and_validation", {}).get("validation_flag_counts", {}).get("done_after_disconnect", 0)
    anomaly_handling_passed = (
        done_after_disc_count == expected_anomalies
        and non_null_charging_in_anomalies == 0
        and non_null_idle_in_anomalies == 0
        and all_flagged
    )

    # ------------------------------------------------------------------
    # 7. Null Counts Summary
    # ------------------------------------------------------------------
    sessions_null_counts = {col: int(sessions_df[col].isna().sum()) for col in sessions_df.columns}
    ui_null_counts = {col: int(user_inputs_df[col].isna().sum()) for col in user_inputs_df.columns}

    # ------------------------------------------------------------------
    # 8. Analytical Queries & Aggregations
    # ------------------------------------------------------------------
    conn_dts = pd.to_datetime(sessions_df["connection_time"], utc=True)
    sessions_df["_dt_utc"] = conn_dts
    sessions_df["_month_str"] = conn_dts.dt.strftime("%Y-%m")
    sessions_df["_date_str"] = conn_dts.dt.strftime("%Y-%m-%d")
    sessions_df["_hour"] = conn_dts.dt.hour

    # A. Monthly Sessions Summary
    monthly_summary = (
        sessions_df.groupby("_month_str")
        .agg(
            total_sessions=("id", "count"),
            total_kwh_delivered=("kwh_delivered", "sum"),
            avg_kwh_per_session=("kwh_delivered", "mean"),
            avg_session_duration_min=("session_duration_min", "mean"),
            median_session_duration_min=("session_duration_min", "median"),
            unique_users=("user_id", lambda s: s.dropna().nunique()),
            unique_stations=("station_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"_month_str": "year_month"})
    )
    monthly_csv_path = val_dir / "monthly_sessions_summary.csv"
    monthly_summary.to_csv(monthly_csv_path, index=False)

    # B. Daily Sessions Summary
    daily_summary = (
        sessions_df.groupby("_date_str")
        .agg(
            total_sessions=("id", "count"),
            total_kwh_delivered=("kwh_delivered", "sum"),
            avg_session_duration_min=("session_duration_min", "mean"),
        )
        .reset_index()
        .rename(columns={"_date_str": "date"})
    )
    daily_csv_path = val_dir / "daily_sessions_summary.csv"
    daily_summary.to_csv(daily_csv_path, index=False)

    # C. Station Summary
    station_summary = (
        sessions_df.groupby("station_id")
        .agg(
            total_sessions=("id", "count"),
            total_kwh_delivered=("kwh_delivered", "sum"),
            avg_kwh_per_session=("kwh_delivered", "mean"),
            avg_duration_min=("session_duration_min", "mean"),
            median_duration_min=("session_duration_min", "median"),
            unique_spaces=("space_id", "nunique"),
        )
        .sort_values(by="total_sessions", ascending=False)
        .reset_index()
    )
    station_csv_path = val_dir / "station_summary.csv"
    station_summary.to_csv(station_csv_path, index=False)

    # D. Peak Charging Hours Summary
    hourly_summary = (
        sessions_df.groupby("_hour")
        .agg(
            total_sessions=("id", "count"),
            total_kwh_delivered=("kwh_delivered", "sum"),
            avg_kwh_per_session=("kwh_delivered", "mean"),
        )
        .reset_index()
        .rename(columns={"_hour": "charging_hour"})
        .sort_values(by="charging_hour")
    )
    hourly_csv_path = val_dir / "hourly_peak_summary.csv"
    hourly_summary.to_csv(hourly_csv_path, index=False)

    # E. Overall Energy & Duration Metrics
    energy_metrics = {
        "total_delivered_kwh": round(float(sessions_df["kwh_delivered"].sum()), 2),
        "mean_kwh_per_session": round(float(sessions_df["kwh_delivered"].mean()), 2),
        "median_kwh_per_session": round(float(sessions_df["kwh_delivered"].median()), 2),
        "min_kwh_delivered": round(float(sessions_df["kwh_delivered"].min()), 3),
        "max_kwh_delivered": round(float(sessions_df["kwh_delivered"].max()), 3),
        "std_kwh_delivered": round(float(sessions_df["kwh_delivered"].std()), 2) if len(sessions_df) > 1 else 0.0,
        "session_duration_minutes": {
            "mean": round(float(sessions_df["session_duration_min"].mean()), 2),
            "median": round(float(sessions_df["session_duration_min"].median()), 2),
            "min": round(float(sessions_df["session_duration_min"].min()), 2),
            "max": round(float(sessions_df["session_duration_min"].max()), 2),
            "p25": round(float(sessions_df["session_duration_min"].quantile(0.25)), 2),
            "p75": round(float(sessions_df["session_duration_min"].quantile(0.75)), 2),
        },
        "charging_duration_minutes_valid_subset": {
            "count": int(sessions_df["charging_duration_min"].notna().sum()),
            "mean": round(float(sessions_df["charging_duration_min"].dropna().mean()), 2) if sessions_df["charging_duration_min"].notna().any() else 0.0,
            "median": round(float(sessions_df["charging_duration_min"].dropna().median()), 2) if sessions_df["charging_duration_min"].notna().any() else 0.0,
        },
        "idle_duration_minutes_valid_subset": {
            "count": int(sessions_df["idle_duration_min"].notna().sum()),
            "mean": round(float(sessions_df["idle_duration_min"].dropna().mean()), 2) if sessions_df["idle_duration_min"].notna().any() else 0.0,
            "median": round(float(sessions_df["idle_duration_min"].dropna().median()), 2) if sessions_df["idle_duration_min"].notna().any() else 0.0,
        },
    }
    energy_metrics_path = val_dir / "energy_and_duration_metrics.json"
    energy_metrics_path.write_text(json.dumps(energy_metrics, indent=2), encoding="utf-8")

    # Clean up temporary helper columns
    sessions_df.drop(columns=["_dt_utc", "_month_str", "_date_str", "_hour"], inplace=True)

    # ------------------------------------------------------------------
    # 9. Master Verification Report
    # ------------------------------------------------------------------
    all_checks_passed = bool(
        reconciliation_passed
        and uniqueness_passed
        and referential_integrity_passed
        and utc_consistency_passed
        and non_negative_passed
        and anomaly_handling_passed
    )

    verification_report = {
        "verification_status": "PASSED" if all_checks_passed else "FAILED",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "all_checks_passed": all_checks_passed,
        "staging_readiness": staging_res,
        "checks": {
            "reconciliation": {
                "passed": reconciliation_passed,
                "raw_input_sessions": raw_input_count,
                "clean_session_records": len(sessions_df),
                "quarantined_records": expected_quarantined,
                "raw_user_inputs_count": raw_uis_count,
                "clean_user_inputs_count": len(user_inputs_df),
            },
            "identifier_uniqueness": {
                "passed": uniqueness_passed,
                "unique_session_ids": unique_session_ids,
                "unique_ids": unique_ids,
                "duplicate_session_ids": duplicate_session_ids,
                "duplicate_ids": duplicate_ids,
            },
            "referential_integrity": {
                "passed": referential_integrity_passed,
                "user_inputs_total_rows": len(user_inputs_df),
                "distinct_sessions_in_user_inputs": len(ui_session_id_set),
                "orphaned_foreign_keys": len(orphaned_uis),
            },
            "timestamp_utc_compliance": {
                "passed": utc_consistency_passed,
                "non_utc_counts": non_utc_counts,
                "earliest_timestamp": str(sessions_df["connection_time"].min()),
                "latest_timestamp": str(sessions_df["connection_time"].max()),
            },
            "non_negative_values": {
                "passed": non_negative_passed,
                "negative_session_durations": negative_session_dur,
                "negative_charging_durations": negative_charging_dur,
                "negative_idle_durations": negative_idle_dur,
                "negative_kwh_delivered": negative_kwh,
            },
            "anomaly_isolation_done_after_disconnect": {
                "passed": anomaly_handling_passed,
                "anomaly_row_count": done_after_disc_count,
                "charging_duration_nulled_count": done_after_disc_count - non_null_charging_in_anomalies,
                "idle_duration_nulled_count": done_after_disc_count - non_null_idle_in_anomalies,
                "all_tagged_with_flag": all_flagged,
            },
        },
        "null_counts": {
            "sessions": sessions_null_counts,
            "user_inputs": ui_null_counts,
        },
        "analytics_summary": {
            "total_energy_delivered_kwh": energy_metrics["total_delivered_kwh"],
            "avg_session_duration_min": energy_metrics["session_duration_minutes"]["mean"],
            "median_session_duration_min": energy_metrics["session_duration_minutes"]["median"],
            "active_stations_count": int(sessions_df["station_id"].nunique()),
            "active_months_count": len(monthly_summary),
            "active_days_count": len(daily_summary),
        },
        "generated_artifacts": {
            "verification_report": str(val_dir / "verification_report.json"),
            "monthly_summary_csv": str(monthly_csv_path),
            "daily_summary_csv": str(daily_csv_path),
            "station_summary_csv": str(station_csv_path),
            "hourly_peak_summary_csv": str(hourly_csv_path),
            "energy_and_duration_metrics_json": str(energy_metrics_path),
        },
    }

    report_path = val_dir / "verification_report.json"
    report_path.write_text(json.dumps(verification_report, indent=2), encoding="utf-8")
    logger.info("Verification report successfully generated at: %s", report_path)

    return verification_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify historical batch ETL outputs and assert staging readiness.")
    parser.add_argument("--historical-dir", type=str, default=str(DEFAULT_HISTORICAL_DIR), help="Historical batch directory")
    parser.add_argument("--candidate", type=str, default=str(DEFAULT_RAW_CANDIDATE), help="Raw candidate JSON file")
    parser.add_argument("--validation-dir", type=str, default=str(DEFAULT_VALIDATION_DIR), help="Validation outputs directory")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    rep = verify_historical_batch(
        historical_dir=args.historical_dir,
        raw_candidate_path=args.candidate,
        validation_dir=args.validation_dir,
    )
    print(json.dumps(rep, indent=2))


if __name__ == "__main__":
    main()
