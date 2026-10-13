"""
streaming/event_generator.py — Simulate real-time EV charging events.

Owner: Harikrishnan SM

Generates synthetic charging events that mimic the structure of real
station telemetry. Uses the shared event schema defined in
contracts/data_contracts.yaml.
"""

import random
import time
from datetime import datetime, timezone


def generate_event() -> dict:
    """
    Generate a single simulated charging event.

    Returns
    -------
    dict
        An event record conforming to the streaming event contract.
    """
    # TODO: Align field names with data_contracts.yaml after dataset inspection
    return {
        "event_id": f"evt-{random.randint(100000, 999999)}",
        "station_id": f"station-{random.randint(1, 50)}",
        "event_type": random.choice(["session_start", "session_end", "energy_update"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "energy_kwh": round(random.uniform(0.0, 75.0), 2),
    }


def stream_events(interval_seconds: float = 1.0):
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
