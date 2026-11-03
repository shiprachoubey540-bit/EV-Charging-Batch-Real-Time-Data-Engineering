"""
database/load_historical.py
Loads the historical Parquet files into PostgreSQL (ev_charging_db).

Safe to re-run: it empties dim_station, charging_sessions and
session_user_inputs first (this also removes the seed test rows).
It does NOT touch charging_events or station_window_metrics.
Everything runs in one transaction. If any count does not match
Ananthan's report, it rolls back and loads nothing.

Run from the project root:  python -m database.load_historical
"""
import getpass
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
HIST = ROOT / "data" / "processed" / "historical_batch"
VALID = ROOT / "data" / "processed" / "validation"

# Numbers from Ananthan's verification report
EXPECTED = {"sessions": 16125, "user_inputs": 16600,
            "stations": 55, "total_kwh": 152572.77}

SESSION_COLS = [
    "id", "session_id", "site_id", "cluster_id", "station_id", "space_id",
    "connection_time", "disconnect_time", "done_charging_time",
    "kwh_delivered", "user_id", "timezone", "session_duration_min",
    "charging_duration_min", "idle_duration_min", "validation_flags",
    "ingestion_timestamp",
]
SESSION_TIME_COLS = ["connection_time", "disconnect_time",
                     "done_charging_time", "ingestion_timestamp"]

INPUT_COLS = [
    "session_id", "user_id_numeric", "wh_per_mile", "kwh_requested",
    "miles_requested", "minutes_available", "modified_at",
    "payment_required", "requested_departure",
]
INPUT_TIME_COLS = ["modified_at", "requested_departure"]


def to_rows(df, cols, time_cols):
    """Convert a DataFrame to a list of tuples, with NaN/NaT as None."""
    df = df[cols].copy()
    for c in time_cols:
        df[c] = pd.to_datetime(df[c], utc=True)
    df = df.astype(object).where(df.notna(), None)
    return list(df.itertuples(index=False, name=None))


def connect():
    password = os.getenv("POSTGRES_PASSWORD") or getpass.getpass(
        "PostgreSQL password: ")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "ev_charging_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=password,
    )


def main():
    sessions = pd.read_parquet(HIST / "sessions.parquet")
    inputs = pd.read_parquet(HIST / "user_inputs.parquet")
    summary = pd.read_csv(VALID / "station_summary.csv")
    print(f"Read {len(sessions)} sessions, {len(inputs)} user inputs, "
          f"{len(summary)} stations in summary")

    # Stations: ids from station_summary.csv, other columns from sessions
    stations = (sessions[["station_id", "space_id", "site_id", "cluster_id"]]
                .drop_duplicates("station_id"))
    if set(stations.station_id) != set(summary.station_id):
        sys.exit("ERROR: stations in sessions.parquet and "
                 "station_summary.csv do not match")
    station_rows = to_rows(stations,
                           ["station_id", "space_id", "site_id", "cluster_id"],
                           [])

    session_rows = to_rows(sessions, SESSION_COLS, SESSION_TIME_COLS)
    input_rows = to_rows(inputs, INPUT_COLS, INPUT_TIME_COLS)

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE session_user_inputs, charging_sessions, "
                        "dim_station CASCADE;")
            execute_values(
                cur,
                "INSERT INTO dim_station "
                "(station_id, space_id, site_id, cluster_id) VALUES %s",
                station_rows)
            execute_values(
                cur,
                f"INSERT INTO charging_sessions ({', '.join(SESSION_COLS)}) "
                "VALUES %s", session_rows, page_size=1000)
            execute_values(
                cur,
                f"INSERT INTO session_user_inputs ({', '.join(INPUT_COLS)}) "
                "VALUES %s", input_rows, page_size=1000)

            # Reconciliation against Ananthan's report
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT station_id), "
                        "ROUND(SUM(kwh_delivered)::numeric, 2) "
                        "FROM charging_sessions")
            n_sessions, n_stations, total_kwh = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM session_user_inputs")
            n_inputs = cur.fetchone()[0]

        actual = {"sessions": n_sessions, "user_inputs": n_inputs,
                  "stations": n_stations, "total_kwh": float(total_kwh)}
        print("\nReconciliation (expected vs loaded):")
        ok = True
        for key, exp in EXPECTED.items():
            match = abs(actual[key] - exp) < 0.01
            ok = ok and match
            print(f"  {key:12} {exp:>12} {actual[key]:>12}  "
                  f"{'PASS' if match else 'FAIL'}")
        if not ok:
            conn.rollback()
            sys.exit("\nMismatch. Rolled back, nothing was loaded.")
        conn.commit()
        print("\nLoad committed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()