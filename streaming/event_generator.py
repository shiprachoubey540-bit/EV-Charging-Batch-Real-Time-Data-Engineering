"""Generate simulated EV charging events from the verified station fleet."""

from __future__ import annotations

import csv
import logging
import random
import time
import uuid
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SESSIONS_PARQUET_PATH = (
    _PROJECT_ROOT / "data" / "processed" / "historical_batch" / "sessions.parquet"
)
_TEST_STATION_IDS_PATH = (
    Path(__file__).resolve().parent / "testdata" / "verified_station_ids.csv"
)
_default_event_stream: Iterator[dict[str, object]] | None = None


def _read_station_ids_csv(path: Path) -> tuple[str, ...]:
    """Read station IDs from the single-column local test fixture."""
    with path.open(newline="", encoding="utf-8-sig") as station_file:
        reader = csv.reader(station_file)
        header = next(reader, [])
        if header != ["station_id"]:
            raise ValueError(f"{path} must contain only a station_id column.")
        return tuple(row[0] if len(row) == 1 else "" for row in reader)


def _read_station_ids_from_parquet(path: Path) -> tuple[str, ...]:
    """Load distinct station IDs from the historical sessions Parquet file."""
    try:
        import pyarrow.parquet as parquet
    except ImportError as error:
        raise ImportError(
            "Reading the historical sessions Parquet file requires pyarrow; "
            "install streaming/requirements.txt."
        ) from error

    schema = parquet.read_schema(path)
    if "station_id" not in schema.names:
        raise ValueError(f"{path} does not contain a station_id column.")
    station_values = parquet.read_table(path, columns=["station_id"])[
        "station_id"
    ].drop_null().to_pylist()
    return tuple(dict.fromkeys(station_values))


def _validate_station_ids(station_ids: Sequence[str]) -> tuple[str, ...]:
    if isinstance(station_ids, str) or not isinstance(station_ids, Sequence):
        raise ValueError("station_ids must be a sequence of IDs, not a single string.")
    if any(not isinstance(station_id, str) for station_id in station_ids):
        raise ValueError("Station IDs must be strings.")
    resolved_ids = tuple(station_id.strip() for station_id in station_ids)
    if not resolved_ids or any(not station_id for station_id in resolved_ids):
        raise ValueError("Station IDs must be a non-empty list of non-empty strings.")
    if any(len(station_id) > 20 for station_id in resolved_ids):
        raise ValueError("Station IDs must not exceed 20 characters.")
    if len(set(resolved_ids)) != len(resolved_ids):
        raise ValueError("Station IDs must not contain duplicates.")
    return resolved_ids


@lru_cache(maxsize=1)
def _load_verified_station_ids(path: Path) -> tuple[str, ...]:
    return _validate_station_ids(_read_station_ids_from_parquet(path))


def resolve_station_ids(
    station_ids: Sequence[str] | None = None,
    *,
    local_testing: bool = False,
) -> tuple[str, ...]:
    """Resolve explicit IDs or distinct IDs from the historical sessions data."""
    if station_ids is not None:
        return _validate_station_ids(station_ids)

    try:
        return _load_verified_station_ids(_SESSIONS_PARQUET_PATH)
    except FileNotFoundError:
        if not local_testing:
            raise FileNotFoundError(
                "Historical sessions Parquet file not found at "
                f"{_SESSIONS_PARQUET_PATH}. Run the historical batch "
                "verification first, or enable local testing to use the "
                "bundled verified station-ID fixture."
            ) from None
        logger.warning(
            "LOCAL TESTING MODE: historical sessions Parquet is unavailable; "
            "using the bundled verified station-ID fixture."
        )
        return _validate_station_ids(_read_station_ids_csv(_TEST_STATION_IDS_PATH))


def generate_session_events(
    station_id: str,
    *,
    session_id: str | None = None,
    update_count: int | None = None,
    start_time: datetime | None = None,
) -> Iterator[dict[str, object]]:
    """Yield one complete session lifecycle for a single station."""
    if not isinstance(station_id, str) or not station_id.strip():
        raise ValueError("station_id must be a non-empty string.")
    if len(station_id) > 20:
        raise ValueError("station_id must not exceed 20 characters.")
    if session_id is not None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must be a non-empty string.")
        if len(session_id) > 64:
            raise ValueError("session_id must not exceed 64 characters.")
    if update_count is None:
        update_count = random.randint(1, 3)
    if update_count < 1:
        raise ValueError("update_count must be at least 1.")
    if start_time is None:
        start_time = datetime.now(timezone.utc)
    elif start_time.tzinfo is None:
        raise ValueError("start_time must include timezone information.")
    start_time = start_time.astimezone(timezone.utc)

    active_session_id = session_id if session_id is not None else str(uuid.uuid4())
    cumulative_energy = 0.0
    event_types = [
        "session_start",
        *("energy_update" for _ in range(update_count)),
        "session_end",
    ]
    for event_index, event_type in enumerate(event_types):
        incremental_energy = (
            0.0 if event_type == "session_start" else round(random.uniform(0.5, 5.0), 2)
        )
        cumulative_energy = round(cumulative_energy + incremental_energy, 2)
        event_time = start_time + timedelta(seconds=event_index)
        yield {
            "event_id": str(uuid.uuid4()),
            "station_id": station_id,
            "session_id": active_session_id,
            "event_type": event_type,
            "timestamp": event_time.isoformat(),
            "energy_kwh": cumulative_energy,
            "incremental_kwh": incremental_energy,
        }


def generate_event(
    event_id: str | None = None,
    station_id: str | None = None,
    session_id: str | None = None,
    event_type: str | None = None,
    timestamp: str | None = None,
    energy_kwh: float | None = None,
    incremental_kwh: float | None = None,
    cumulative_kwh: float | None = None,
) -> dict[str, object]:
    """Generate one event, honoring the optional arguments supported by main."""
    if all(
        value is None
        for value in (
            event_id,
            station_id,
            session_id,
            event_type,
            timestamp,
            energy_kwh,
            incremental_kwh,
            cumulative_kwh,
        )
    ):
        global _default_event_stream
        if _default_event_stream is None:
            _default_event_stream = stream_events(interval_seconds=0, local_testing=True)
        return next(_default_event_stream)

    active_station_id = station_id or random.choice(
        resolve_station_ids(local_testing=True)
    )
    active_event_type = event_type or random.choice(
        ["session_start", "energy_update", "session_end"]
    )
    now_utc = datetime.now(timezone.utc)
    active_timestamp = timestamp or now_utc.isoformat()
    active_session_id = session_id or (
        f"{active_station_id}_{now_utc.strftime('%Y%m%d%H%M%S')}"
    )
    if cumulative_kwh is not None and energy_kwh is None:
        energy_kwh = cumulative_kwh
    if energy_kwh is None:
        energy_kwh = (
            0.0
            if active_event_type == "session_start"
            else round(random.uniform(0.1, 75.0), 2)
        )
    if incremental_kwh is None:
        incremental_kwh = (
            0.0
            if active_event_type == "session_start"
            else round(min(energy_kwh, random.uniform(0.0, 5.0)), 2)
        )
    return {
        "event_id": event_id or f"evt-{uuid.uuid4()}",
        "station_id": active_station_id,
        "session_id": active_session_id,
        "event_type": active_event_type,
        "timestamp": active_timestamp,
        "energy_kwh": float(energy_kwh),
        "incremental_kwh": float(incremental_kwh),
    }


def stream_events(
    interval_seconds: float = 1.0,
    *,
    station_ids: Sequence[str] | None = None,
    local_testing: bool = False,
) -> Iterator[dict[str, object]]:
    """Continuously yield ordered, complete session lifecycles."""
    if interval_seconds < 0:
        raise ValueError("interval_seconds must not be negative.")
    available_station_ids = resolve_station_ids(
        station_ids,
        local_testing=local_testing,
    )
    next_start_time = datetime.now(timezone.utc)

    while True:
        events = list(
            generate_session_events(
                random.choice(available_station_ids),
                start_time=next_start_time,
            )
        )
        for event in events:
            yield event
            if interval_seconds:
                time.sleep(interval_seconds)
        last_timestamp = datetime.fromisoformat(str(events[-1]["timestamp"]))
        next_start_time = last_timestamp + timedelta(seconds=1)


if __name__ == "__main__":
    for generated_event in stream_events(local_testing=True):
        print(generated_event)
