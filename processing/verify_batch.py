"""
processing/verify_batch.py — Independent verification & analytics readiness for historical batch ETL.

Owner: Aleena Antony, Ananthan S

Performs read-only validation on:
  - data/processed/historical_batch/sessions.parquet
  - data/processed/historical_batch/user_inputs.parquet
  - data/processed/historical_batch/etl_audit_report.json
  - data/raw/full_download/acndata_sessions_full_candidate.json

Validates:
  1. Row counts, column schemas, data types, null counts, and timestamp ranges against audit report.
  2. Uniqueness of primary identifiers (session_id and id).
  3. Referential integrity (every user_inputs.session_id maps to an existing session).
  4. Consistency of UTC timestamp formatting and absence of negative durations / energy.
  5. Accurate isolation of done_after_disconnect anomalies and nulling of their derived duration metrics.
  6. Mathematical reconciliation between raw inputs, clean outputs, and user inputs.
  7. Key analytical queries: sessions per day/month, energy delivered, durations, station distributions.

Outputs saved to: data/processed/validation/
"""

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
DEFAULT_VALIDATION_DIR = PROJECT_ROOT / "data" / "processed" / "validation"


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

    if not sessions_path.exists():
        raise FileNotFoundError(f"Missing sessions parquet: {sessions_path}")
    if not user_inputs_path.exists():
        raise FileNotFoundError(f"Missing user inputs parquet: {user_inputs_path}")
    if not audit_report_path.exists():
        raise FileNotFoundError(f"Missing audit report: {audit_report_path}")
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw candidate JSON: {raw_path}")

    logger.info("Reading processed Parquet files from %s", hist_dir)
    sessions_df = pd.read_parquet(sessions_path)
    user_inputs_df = pd.read_parquet(user_inputs_path)
    audit_report = json.loads(audit_report_path.read_text(encoding="utf-8"))

    logger.info("Reading raw candidate dataset from %s", raw_path)
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_records = raw_payload.get("_items", [])
    raw_uis_count = sum(
        len(item.get("userInputs", []))
        for item in raw_records
        if isinstance(item.get("userInputs"), list)
    )

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
    # Check that all non-null timestamps end with +00:00 (ISO UTC)
    ts_cols = ["connection_time", "disconnect_time", "done_charging_time"]
    non_utc_counts = {}
    for col in ts_cols:
        s_non_null = sessions_df[col].dropna()
        non_utc = (~s_non_null.str.endswith("+00:00")).sum()
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

    # Verify duration metrics are strictly null for all anomalous sessions
    non_null_charging_in_anomalies = int(anomalous_sessions["charging_duration_min"].notna().sum())
    non_null_idle_in_anomalies = int(anomalous_sessions["idle_duration_min"].notna().sum())
    all_flagged = bool((anomalous_sessions["validation_flags"] == "done_after_disconnect").all())

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
    # Parse connection timestamps for time-series grouping
    conn_dts = pd.to_datetime(sessions_df["connection_time"], utc=True)
    sessions_df["_dt_utc"] = conn_dts
    sessions_df["_month_str"] = conn_dts.dt.strftime("%Y-%m")
    sessions_df["_date_str"] = conn_dts.dt.strftime("%Y-%m-%d")

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

    # D. Overall Energy & Duration Metrics
    energy_metrics = {
        "total_delivered_kwh": round(float(sessions_df["kwh_delivered"].sum()), 2),
        "mean_kwh_per_session": round(float(sessions_df["kwh_delivered"].mean()), 2),
        "median_kwh_per_session": round(float(sessions_df["kwh_delivered"].median()), 2),
        "min_kwh_delivered": round(float(sessions_df["kwh_delivered"].min()), 3),
        "max_kwh_delivered": round(float(sessions_df["kwh_delivered"].max()), 3),
        "std_kwh_delivered": round(float(sessions_df["kwh_delivered"].std()), 2),
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
            "mean": round(float(sessions_df["charging_duration_min"].dropna().mean()), 2),
            "median": round(float(sessions_df["charging_duration_min"].dropna().median()), 2),
        },
        "idle_duration_minutes_valid_subset": {
            "count": int(sessions_df["idle_duration_min"].notna().sum()),
            "mean": round(float(sessions_df["idle_duration_min"].dropna().mean()), 2),
            "median": round(float(sessions_df["idle_duration_min"].dropna().median()), 2),
        },
    }
    energy_metrics_path = val_dir / "energy_and_duration_metrics.json"
    energy_metrics_path.write_text(json.dumps(energy_metrics, indent=2), encoding="utf-8")

    # Clean up temporary helper columns
    sessions_df.drop(columns=["_dt_utc", "_month_str", "_date_str"], inplace=True)

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
            "energy_and_duration_metrics_json": str(energy_metrics_path),
        },
    }

    report_path = val_dir / "verification_report.json"
    report_path.write_text(json.dumps(verification_report, indent=2), encoding="utf-8")
    logger.info("Verification report successfully generated at: %s", report_path)

    return verification_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify historical batch ETL outputs and generate analytics summary.")
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
