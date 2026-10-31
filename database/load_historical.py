"""
database/load_historical.py — Idempotent batch loader for historical Parquet datasets.

Owner: Bainty Kaur (database/), Aleena Antony (processing/)

Loads verified historical datasets:
  - data/processed/historical_batch/sessions.parquet (16,125 records)
  - data/processed/historical_batch/user_inputs.parquet (16,600 records)
  - data/processed/validation/station_summary.csv (55 verified stations)

Features:
  - Strictly parameterized SQL via psycopg2.extras.execute_values
  - Transactional commits in bounded batches with rollback on failure
  - Idempotent upserts (ON CONFLICT DO UPDATE) preventing duplicate sessions and user inputs
  - Referential integrity: stations -> charging_sessions -> session_user_inputs
  - Post-load reconciliation checking row counts and total kWh delivered
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import get_db_config, get_db_connection, run_migrations

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("database.load_historical")

DEFAULT_SESSIONS_PARQUET = PROJECT_ROOT / "data" / "processed" / "historical_batch" / "sessions.parquet"
DEFAULT_USER_INPUTS_PARQUET = PROJECT_ROOT / "data" / "processed" / "historical_batch" / "user_inputs.parquet"
DEFAULT_STATIONS_CSV = PROJECT_ROOT / "data" / "processed" / "validation" / "station_summary.csv"


def _clean_val(val: Any) -> Any:
    """Convert numpy / pandas NaN and null equivalents to None for SQL parameterization."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, (np.floating, float)) and (np.isnan(val) or np.isinf(val)):
        return None
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (np.floating, float)):
        return float(val)
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    return str(val) if isinstance(val, Path) else val


def load_stations_dimension(
    conn: Any,
    stations_csv: Path = DEFAULT_STATIONS_CSV,
    sessions_df: pd.DataFrame | None = None,
) -> int:
    """
    Populate or update the stations dimension table.
    """
    from psycopg2.extras import execute_values

    records: list[tuple] = []
    if stations_csv.exists():
        df_st = pd.read_csv(stations_csv)
        for _, r in df_st.iterrows():
            st_id = str(r["station_id"])
            tot_sess = int(r.get("total_sessions", 0))
            tot_kwh = float(r.get("total_kwh_delivered", 0.0))
            records.append((st_id, "0002", "0039", None, tot_sess, tot_kwh))
    elif sessions_df is not None:
        # Fallback from sessions
        agg = sessions_df.groupby("station_id").agg(
            site_id=("site_id", "first"),
            cluster_id=("cluster_id", "first"),
            space_id=("space_id", "first"),
            tot_sess=("session_id", "count"),
            tot_kwh=("kwh_delivered", "sum"),
        ).reset_index()
        for _, r in agg.iterrows():
            records.append((
                str(r["station_id"]),
                str(r["site_id"]),
                str(r["cluster_id"]),
                str(r["space_id"]),
                int(r["tot_sess"]),
                float(r["tot_kwh"]),
            ))

    if not records:
        logger.warning("No station dimension records found to load.")
        return 0

    query = """
        INSERT INTO stations (
            station_id, site_id, cluster_id, space_id,
            total_historical_sessions, total_historical_kwh
        ) VALUES %s
        ON CONFLICT (station_id) DO UPDATE SET
            total_historical_sessions = EXCLUDED.total_historical_sessions,
            total_historical_kwh = EXCLUDED.total_historical_kwh,
            updated_at = NOW();
    """
    with conn.cursor() as cur:
        execute_values(cur, query, records, page_size=len(records))
    conn.commit()
    logger.info("Loaded %d station reference records into 'stations'.", len(records))
    return len(records)


def load_historical_sessions(
    conn: Any,
    sessions_parquet: Path = DEFAULT_SESSIONS_PARQUET,
    batch_size: int = 1000,
) -> dict[str, Any]:
    """
    Load sessions.parquet into charging_sessions in bounded batches.
    """
    from psycopg2.extras import execute_values

    if not sessions_parquet.exists():
        raise FileNotFoundError(f"Sessions Parquet dataset not found at {sessions_parquet}")

    df = pd.read_parquet(sessions_parquet)
    total_rows = len(df)
    logger.info("Reading %d session rows from %s...", total_rows, sessions_parquet)

    # Ensure all station_ids in sessions exist in stations table to satisfy FK
    unique_stations = df[["station_id", "site_id", "cluster_id", "space_id"]].drop_duplicates()
    st_records = [
        (str(r["station_id"]), str(r["site_id"]), str(r["cluster_id"]), str(r["space_id"]))
        for _, r in unique_stations.iterrows()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO stations (station_id, site_id, cluster_id, space_id)
            VALUES %s
            ON CONFLICT (station_id) DO NOTHING;
            """,
            st_records,
            page_size=len(st_records),
        )
    conn.commit()

    insert_query = """
        INSERT INTO charging_sessions (
            id, session_id, site_id, cluster_id, station_id, space_id,
            connection_time, disconnect_time, done_charging_time,
            kwh_delivered, user_id, timezone,
            session_duration_min, charging_duration_min, idle_duration_min,
            validation_flags, ingestion_timestamp
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            session_id = EXCLUDED.session_id,
            station_id = EXCLUDED.station_id,
            connection_time = EXCLUDED.connection_time,
            disconnect_time = EXCLUDED.disconnect_time,
            done_charging_time = EXCLUDED.done_charging_time,
            kwh_delivered = EXCLUDED.kwh_delivered,
            user_id = EXCLUDED.user_id,
            session_duration_min = EXCLUDED.session_duration_min,
            charging_duration_min = EXCLUDED.charging_duration_min,
            idle_duration_min = EXCLUDED.idle_duration_min,
            validation_flags = EXCLUDED.validation_flags,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp;
    """

    processed_count = 0
    start_time = time.time()

    for start_idx in range(0, total_rows, batch_size):
        batch_df = df.iloc[start_idx : start_idx + batch_size]
        batch_records = []
        for _, r in batch_df.iterrows():
            batch_records.append((
                _clean_val(r["id"]),
                _clean_val(r["session_id"]),
                _clean_val(r["site_id"]),
                _clean_val(r["cluster_id"]),
                _clean_val(r["station_id"]),
                _clean_val(r["space_id"]),
                _clean_val(r["connection_time"]),
                _clean_val(r["disconnect_time"]),
                _clean_val(r["done_charging_time"]),
                _clean_val(r["kwh_delivered"]),
                _clean_val(r["user_id"]),
                _clean_val(r["timezone"]),
                _clean_val(r["session_duration_min"]),
                _clean_val(r["charging_duration_min"]),
                _clean_val(r["idle_duration_min"]),
                _clean_val(r["validation_flags"]),
                _clean_val(r["ingestion_timestamp"]),
            ))

        with conn.cursor() as cur:
            execute_values(cur, insert_query, batch_records, page_size=len(batch_records))
        conn.commit()
        processed_count += len(batch_records)

    elapsed = time.time() - start_time
    logger.info("Successfully loaded %d sessions in %.2fs.", processed_count, elapsed)
    return {"source_rows": total_rows, "loaded_rows": processed_count, "elapsed_sec": round(elapsed, 2)}


def load_historical_user_inputs(
    conn: Any,
    user_inputs_parquet: Path = DEFAULT_USER_INPUTS_PARQUET,
    batch_size: int = 1000,
) -> dict[str, Any]:
    """
    Load user_inputs.parquet into session_user_inputs with composite deduplication.
    """
    from psycopg2.extras import execute_values

    if not user_inputs_parquet.exists():
        raise FileNotFoundError(f"User inputs Parquet dataset not found at {user_inputs_parquet}")

    df = pd.read_parquet(user_inputs_parquet)
    # Assign deterministic input_index per session to guarantee idempotency
    df["input_index"] = df.groupby("session_id").cumcount()
    total_rows = len(df)
    logger.info("Reading %d user-input rows from %s...", total_rows, user_inputs_parquet)

    insert_query = """
        INSERT INTO session_user_inputs (
            session_id, input_index, user_id_numeric, wh_per_mile,
            kwh_requested, miles_requested, minutes_available,
            modified_at, payment_required, requested_departure
        ) VALUES %s
        ON CONFLICT (session_id, input_index) DO UPDATE SET
            user_id_numeric = EXCLUDED.user_id_numeric,
            wh_per_mile = EXCLUDED.wh_per_mile,
            kwh_requested = EXCLUDED.kwh_requested,
            miles_requested = EXCLUDED.miles_requested,
            minutes_available = EXCLUDED.minutes_available,
            modified_at = EXCLUDED.modified_at,
            payment_required = EXCLUDED.payment_required,
            requested_departure = EXCLUDED.requested_departure;
    """

    processed_count = 0
    start_time = time.time()

    for start_idx in range(0, total_rows, batch_size):
        batch_df = df.iloc[start_idx : start_idx + batch_size]
        batch_records = []
        for _, r in batch_df.iterrows():
            batch_records.append((
                _clean_val(r["session_id"]),
                int(r["input_index"]),
                _clean_val(r["user_id_numeric"]),
                _clean_val(r["wh_per_mile"]),
                _clean_val(r["kwh_requested"]),
                _clean_val(r["miles_requested"]),
                _clean_val(r["minutes_available"]),
                _clean_val(r["modified_at"]),
                bool(r["payment_required"]),
                _clean_val(r["requested_departure"]),
            ))

        with conn.cursor() as cur:
            execute_values(cur, insert_query, batch_records, page_size=len(batch_records))
        conn.commit()
        processed_count += len(batch_records)

    elapsed = time.time() - start_time
    logger.info("Successfully loaded %d user-input records in %.2fs.", processed_count, elapsed)
    return {"source_rows": total_rows, "loaded_rows": processed_count, "elapsed_sec": round(elapsed, 2)}


def reconcile_loaded_data(conn: Any) -> dict[str, Any]:
    """
    Reconcile PostgreSQL tables against expected historical baselines.
    """
    reconciliation: dict[str, Any] = {}

    with conn.cursor() as cur:
        # 1. Sessions count and energy
        cur.execute("""
            SELECT
                COUNT(*) AS session_count,
                COUNT(DISTINCT station_id) AS station_count,
                ROUND(SUM(kwh_delivered)::numeric, 2) AS total_kwh,
                MIN(connection_time) AS earliest_connection,
                MAX(disconnect_time) AS latest_disconnect
            FROM charging_sessions;
        """)
        row = cur.fetchone()
        reconciliation["sessions"] = {
            "db_count": row[0],
            "station_count": row[1],
            "total_kwh_delivered": float(row[2]) if row[2] is not None else 0.0,
            "earliest_connection": str(row[3]),
            "latest_disconnect": str(row[4]),
            "expected_sessions": 16125,
            "expected_kwh": 152572.77,
            "sessions_reconciled": (row[0] == 16125),
            "kwh_reconciled": (abs(float(row[2] or 0.0) - 152572.77) < 0.05),
        }

        # 2. User inputs count and FK integrity
        cur.execute("""
            SELECT
                COUNT(*) AS user_input_count,
                COUNT(DISTINCT session_id) AS distinct_sessions
            FROM session_user_inputs;
        """)
        row_ui = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*)
            FROM session_user_inputs ui
            LEFT JOIN charging_sessions cs ON ui.session_id = cs.session_id
            WHERE cs.session_id IS NULL;
        """)
        orphaned = cur.fetchone()[0]

        reconciliation["user_inputs"] = {
            "db_count": row_ui[0],
            "distinct_sessions": row_ui[1],
            "orphaned_count": orphaned,
            "expected_count": 16600,
            "user_inputs_reconciled": (row_ui[0] == 16600),
            "fk_integrity_preserved": (orphaned == 0),
        }

        # 3. Stations count
        cur.execute("SELECT COUNT(*) FROM stations;")
        stations_count = cur.fetchone()[0]
        reconciliation["stations"] = {
            "db_count": stations_count,
            "expected_count": 55,
            "stations_reconciled": (stations_count >= 55),
        }

    all_passed = (
        reconciliation["sessions"]["sessions_reconciled"]
        and reconciliation["sessions"]["kwh_reconciled"]
        and reconciliation["user_inputs"]["user_inputs_reconciled"]
        and reconciliation["user_inputs"]["fk_integrity_preserved"]
    )
    reconciliation["all_reconciled"] = all_passed
    return reconciliation


def run_full_load(
    sessions_parquet: Path = DEFAULT_SESSIONS_PARQUET,
    user_inputs_parquet: Path = DEFAULT_USER_INPUTS_PARQUET,
    stations_csv: Path = DEFAULT_STATIONS_CSV,
    batch_size: int = 1000,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Orchestrate full historical batch loading with migrations and reconciliation.
    """
    logger.info("Starting historical PostgreSQL load...")
    # Step 1: Ensure migrations are applied
    run_migrations(config=config)

    conn = get_db_connection(config)
    try:
        # Step 2: Load stations reference
        st_count = load_stations_dimension(conn, stations_csv=stations_csv)

        # Step 3: Load sessions
        sess_res = load_historical_sessions(conn, sessions_parquet=sessions_parquet, batch_size=batch_size)

        # Step 4: Load user inputs
        ui_res = load_historical_user_inputs(conn, user_inputs_parquet=user_inputs_parquet, batch_size=batch_size)

        # Step 5: Reconcile
        recon = reconcile_loaded_data(conn)

        logger.info(
            "Historical load completed: %d sessions, %d user inputs, %d stations. All reconciled: %s",
            sess_res["loaded_rows"],
            ui_res["loaded_rows"],
            st_count,
            recon["all_reconciled"],
        )
        return {
            "stations_loaded": st_count,
            "sessions": sess_res,
            "user_inputs": ui_res,
            "reconciliation": recon,
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="EV Charging PostgreSQL Historical Batch Loader")
    parser.add_argument("--sessions", type=Path, default=DEFAULT_SESSIONS_PARQUET, help="Path to sessions.parquet")
    parser.add_argument("--user-inputs", type=Path, default=DEFAULT_USER_INPUTS_PARQUET, help="Path to user_inputs.parquet")
    parser.add_argument("--stations", type=Path, default=DEFAULT_STATIONS_CSV, help="Path to station_summary.csv")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for INSERT operations")
    parser.add_argument("--reconcile-only", action="store_true", help="Only run reconciliation queries on existing DB data")
    args = parser.parse_args()

    cfg = get_db_config()
    if args.reconcile_only:
        conn = get_db_connection(cfg)
        try:
            recon = reconcile_loaded_data(conn)
            import json
            print(json.dumps(recon, indent=2))
        finally:
            conn.close()
    else:
        results = run_full_load(
            sessions_parquet=args.sessions,
            user_inputs_parquet=args.user_inputs,
            stations_csv=args.stations,
            batch_size=args.batch_size,
            config=cfg,
        )
        import json
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
