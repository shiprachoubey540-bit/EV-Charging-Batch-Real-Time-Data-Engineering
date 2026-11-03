"""
streaming/simulate_to_postgres.py — Synthetic event simulator writing directly to PostgreSQL.

Bypasses Kafka entirely. Uses generate_event() from event_generator.py and writes
simulated charging events + window metrics into charging_events and
station_window_metrics tables so the dashboard Real-Time Observatory shows live data.

Usage (from project root):
    python -m streaming.simulate_to_postgres --events 50 --batch-size 10

Owner: Shipra Choubey (dashboard integration)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
except ImportError:
    pass

import psycopg2
from psycopg2.extras import execute_values

from streaming.event_generator import generate_event


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ev_charging_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        connect_timeout=5,
    )


def insert_events(conn, events: list[dict]):
    """Insert a batch of events into charging_events (upsert on event_id)."""
    rows = [
        (
            e["event_id"],
            e["station_id"],
            e["session_id"],
            e["event_type"],
            e["timestamp"],          # ISO string → psycopg2 auto-converts
            float(e.get("energy_kwh") or 0.0),
            float(e.get("incremental_kwh") or 0.0),
        )
        for e in events
    ]
    sql = """
        INSERT INTO charging_events
            (event_id, station_id, session_id, event_type,
             event_timestamp, energy_kwh, incremental_kwh)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


def compute_and_insert_window_metrics(conn, events: list[dict], window_minutes: int = 10):
    """Compute simple tumbling window aggregates and upsert into station_window_metrics."""
    now = datetime.now(timezone.utc)
    window_end = now
    window_start = now - timedelta(minutes=window_minutes)

    # Aggregate per station
    station_stats: dict[str, dict] = defaultdict(lambda: {
        "event_count": 0,
        "total_incremental_kwh": 0.0,
        "max_cumulative_kwh": 0.0,
    })

    for e in events:
        sid = e["station_id"]
        station_stats[sid]["event_count"] += 1
        station_stats[sid]["total_incremental_kwh"] += float(e.get("incremental_kwh") or 0.0)
        station_stats[sid]["max_cumulative_kwh"] = max(
            station_stats[sid]["max_cumulative_kwh"],
            float(e.get("energy_kwh") or 0.0),
        )

    rows = [
        (
            sid,
            window_start,
            window_end,
            stats["event_count"],
            stats["total_incremental_kwh"],
            stats["max_cumulative_kwh"],
        )
        for sid, stats in station_stats.items()
    ]

    sql = """
        INSERT INTO station_window_metrics
            (station_id, window_start, window_end,
             event_count, total_incremental_kwh, max_cumulative_kwh)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


def run(total_events: int = 100, batch_size: int = 10, delay_sec: float = 0.5):
    print(f"[simulate_to_postgres] Connecting to PostgreSQL...")
    conn = get_conn()
    print(f"[simulate_to_postgres] Connected. Generating {total_events} events in batches of {batch_size}...")

    generated = 0
    batch_num = 0

    while generated < total_events:
        batch = [generate_event() for _ in range(min(batch_size, total_events - generated))]
        insert_events(conn, batch)
        n_windows = compute_and_insert_window_metrics(conn, batch)
        generated += len(batch)
        batch_num += 1
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(
            f"  [{ts}] Batch {batch_num}: inserted {len(batch)} events, "
            f"{n_windows} window metrics  (total so far: {generated})"
        )
        if generated < total_events:
            time.sleep(delay_sec)

    # Final counts
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM charging_events")
        ev_count = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM station_window_metrics")
        wm_count = cur.fetchone()[0]

    conn.close()
    print(f"\n[simulate_to_postgres] Done.")
    print(f"  charging_events total rows      : {ev_count}")
    print(f"  station_window_metrics total rows: {wm_count}")
    print(f"\nRefresh the Streamlit dashboard to see updated Real-Time Observatory.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate streaming events into PostgreSQL")
    parser.add_argument("--events", type=int, default=100, help="Total events to generate")
    parser.add_argument("--batch-size", type=int, default=10, help="Events per batch")
    parser.add_argument("--delay", type=float, default=0.3, help="Seconds between batches")
    args = parser.parse_args()
    run(total_events=args.events, batch_size=args.batch_size, delay_sec=args.delay)
