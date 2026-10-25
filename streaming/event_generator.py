"""
streaming/event_generator.py — Simulate real-time EV charging events.

Owner: Harikrishnan SM

Generates synthetic charging events that mimic the structure of real
station telemetry. Uses the shared event schema defined in
contracts/data_contracts.yaml.
"""

from __future__ import annotations

import random
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator, Optional

# Verified Caltech hardware station identifiers from batch validation fleet
VERIFIED_STATION_IDS = [
    "2-39-81-4550",
    "2-39-139-28",
    "2-39-131-30",
    "2-39-123-10",
    "2-39-124-11",
    "2-39-125-12",
    "2-39-126-13",
    "2-39-127-14",
    "2-39-128-15",
    "2-39-129-16",
    "2-39-130-17",
]


def generate_event(
    event_id: Optional[str] = None,
    station_id: Optional[str] = None,
    session_id: Optional[str] = None,
    event_type: Optional[str] = None,
    timestamp: Optional[str] = None,
    energy_kwh: Optional[float] = None,
    incremental_kwh: Optional[float] = None,
    cumulative_kwh: Optional[float] = None,
) -> dict:
    """
    Generate a single simulated charging event conforming to data_contracts.yaml.

    Parameters
    ----------
    event_id : Optional[str]
        Unique event UUID.
    station_id : Optional[str]
        Hardware station identifier.
    session_id : Optional[str]
        Session identifier.
    event_type : Optional[str]
        One of 'session_start', 'energy_update', 'session_end'.
    timestamp : Optional[str]
        ISO 8601 UTC timestamp.
    energy_kwh : Optional[float]
        Cumulative energy delivered (kWh).
    incremental_kwh : Optional[float]
        Energy delivered in immediate reporting interval.
    cumulative_kwh : Optional[float]
        Alias for energy_kwh.

    Returns
    -------
    dict
        An event record conforming to the streaming event contract.
    """
    evt_type = event_type or random.choice(["session_start", "energy_update", "session_end"])
    st_id = station_id or random.choice(VERIFIED_STATION_IDS)
    now_utc = datetime.now(timezone.utc)
    ts = timestamp or now_utc.isoformat()
    sess_id = session_id or f"{st_id}_{now_utc.strftime('%Y%m%d%H%M%S')}"
    e_id = event_id or f"evt-{uuid.uuid4()}"

    if cumulative_kwh is not None and energy_kwh is None:
        energy_kwh = cumulative_kwh

    if energy_kwh is None:
        if evt_type == "session_start":
            energy_kwh = 0.0
        else:
            energy_kwh = round(random.uniform(0.1, 75.0), 2)

    if incremental_kwh is None:
        if evt_type == "session_start":
            incremental_kwh = 0.0
        else:
            incremental_kwh = round(min(energy_kwh, random.uniform(0.0, 5.0)), 2)

    return {
        "event_id": str(e_id),
        "station_id": str(st_id),
        "session_id": str(sess_id),
        "event_type": str(evt_type),
        "timestamp": str(ts),
        "energy_kwh": float(energy_kwh),
        "incremental_kwh": float(incremental_kwh),
    }


def stream_events(interval_seconds: float = 1.0) -> Iterator[dict]:
    """
    Continuously yield simulated events at a given interval.

    Parameters
    ----------
    interval_seconds : float
        Delay between events.
    """
    while True:
        yield generate_event()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    for event in stream_events(interval_seconds=2.0):
        print(event)
