"""Generate simulated EV charging events that follow the streaming contract."""

import logging
import random
import time
import uuid
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_SESSIONS_PARQUET_PATH = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "processed"
    / "historical_batch"
    / "sessions.parquet"
)
_TEST_STATION_IDS_PATH = (
    Path(__file__).resolve().parent / "testdata" / "verified_station_ids.csv"
)

_default_event_stream: Iterator[dict[str, object]] | None = None


def _read_station_ids_csv(path: Path) -> tuple[str, ...]:
    """Read station IDs from the single-column local test fixture."""
    import csv

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


def resolve_station_ids(
    station_ids: Sequence[str] | None = None,
    *,
    local_testing: bool = False,
) -> tuple[str, ...]:
    """Resolve explicit IDs or distinct IDs from the historical sessions data."""
    configured_ids = station_ids
    if configured_ids is None:
        try:
            configured_ids = _read_station_ids_from_parquet(_SESSIONS_PARQUET_PATH)
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
            configured_ids = _read_station_ids_csv(_TEST_STATION_IDS_PATH)

    if isinstance(configured_ids, str) or not isinstance(configured_ids, Sequence):
        raise ValueError("station_ids must be a sequence of IDs, not a single string.")
    if any(not isinstance(station_id, str) for station_id in configured_ids):
        raise ValueError("Station IDs must be strings.")
    resolved_ids = tuple(station_id.strip() for station_id in configured_ids)
    if not resolved_ids or any(not station_id for station_id in resolved_ids):
        raise ValueError("Station IDs must be a non-empty list of non-empty strings.")
    if any(len(station_id) > 20 for station_id in resolved_ids):
        raise ValueError("Station IDs must not exceed 20 characters.")
    if len(set(resolved_ids)) != len(resolved_ids):
        raise ValueError("Station IDs must not contain duplicates.")
    return resolved_ids


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
        event: dict[str, object] = {
            "event_id": str(uuid.uuid4()),
            "station_id": station_id,
            "session_id": active_session_id,
            "event_type": event_type,
            "timestamp": event_time.isoformat(),
            "energy_kwh": cumulative_energy,
        }
        if event_type == "session_start":
            event["incremental_kwh"] = 0.0
        else:
            event["incremental_kwh"] = incremental_energy
        yield event


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


def generate_event() -> dict[str, object]:
    """Return the next event from a stream using verified station IDs."""
    global _default_event_stream
    if _default_event_stream is None:
        _default_event_stream = stream_events(interval_seconds=0, local_testing=True)
    return next(_default_event_stream)


if __name__ == "__main__":
    for generated_event in stream_events(local_testing=True):
        print(generated_event)
