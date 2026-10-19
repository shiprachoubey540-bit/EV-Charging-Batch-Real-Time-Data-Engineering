"""
ingestion/extract.py — Load raw ACN-Data JSON files.

Owner: Ananthan S

This module handles the extraction phase of the batch ETL pipeline.
It reads the ACN-Data JSON export, handles truncated files, and returns
the raw session records plus API metadata.
"""

import json
import logging
import os
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data/raw")
DATA_SAMPLE_DIR = os.getenv("DATA_SAMPLE_DIR", "./data/sample")
DEFAULT_SOURCE = os.path.join(DATA_RAW_DIR, "acndata_sessions.json")


def _check_date_range(sessions: list[dict], metadata: dict) -> dict:
    """
    Compare requested metadata dates with the actual session dates in records.

    Parameters
    ----------
    sessions : list[dict]
        Extracted session records.
    metadata : dict
        API metadata dictionary containing requested 'start' and 'end'.

    Returns
    -------
    dict
        Date range comparison and completeness summary.
    """
    req_start_raw = metadata.get("start")
    req_end_raw = metadata.get("end")

    req_start_dt = None
    req_end_dt = None
    if req_start_raw:
        try:
            req_start_dt = parsedate_to_datetime(req_start_raw).astimezone(timezone.utc)
        except Exception:
            pass
    if req_end_raw:
        try:
            req_end_dt = parsedate_to_datetime(req_end_raw).astimezone(timezone.utc)
        except Exception:
            pass

    actual_start_dt = None
    actual_end_dt = None

    for s in sessions:
        conn = s.get("connectionTime")
        if conn:
            try:
                dt = parsedate_to_datetime(conn).astimezone(timezone.utc)
                if actual_start_dt is None or dt < actual_start_dt:
                    actual_start_dt = dt
                if actual_end_dt is None or dt > actual_end_dt:
                    actual_end_dt = dt
            except Exception:
                continue

    # Determine date range completeness
    date_range_complete = True
    if req_end_dt and actual_end_dt:
        # If actual data ends more than 1 day before requested end, mark incomplete
        diff_days = (req_end_dt - actual_end_dt).total_seconds() / 86400.0
        if diff_days > 1.0:
            date_range_complete = False
    elif req_end_dt and not actual_end_dt:
        date_range_complete = False

    return {
        "requested_start": req_start_dt.isoformat() if req_start_dt else req_start_raw,
        "requested_end": req_end_dt.isoformat() if req_end_dt else req_end_raw,
        "actual_start": actual_start_dt.isoformat() if actual_start_dt else None,
        "actual_end": actual_end_dt.isoformat() if actual_end_dt else None,
        "date_range_complete": date_range_complete,
    }


def _repair_truncated_json(text: str) -> tuple[str, int]:
    """
    Attempt to repair a truncated ACN-Data JSON export in-memory.

    The ACN-Data API sometimes produces files where the ``_items`` array
    is cut short, ending with a trailing comma instead of ``]}``. This
    function strips that comma and appends the missing closing tokens.
    Does NOT modify the source file on disk.

    Parameters
    ----------
    text : str
        Raw file content.

    Returns
    -------
    tuple[str, int]
        Repaired JSON string and number of bytes appended.

    Raises
    ------
    json.JSONDecodeError
        If repair attempts fail.
    """
    # Fast path: already valid JSON
    try:
        json.loads(text)
        return text, 0
    except json.JSONDecodeError:
        pass

    stripped = text.rstrip()
    if stripped.endswith(","):
        stripped = stripped[:-1]

    # Try closing with ]} (closing _items list + root dict)
    for suffix in ["]}", "\n  ]\n}"]:
        candidate = stripped + suffix
        try:
            json.loads(candidate)
            return candidate, len(suffix)
        except json.JSONDecodeError:
            continue

    raise json.JSONDecodeError("Cannot repair truncated JSON", text, len(text))


def extract_sessions(
    source_path: str | None = None,
    allow_recovery: bool = True,
) -> tuple[list[dict], dict]:
    """
    Load charging session records from an ACN-Data JSON export.

    Parameters
    ----------
    source_path : str | None
        Path to the JSON file. Defaults to ``data/raw/acndata_sessions.json``.
    allow_recovery : bool
        If True, attempts safe in-memory repair on truncated JSON while
        tagging the dataset status explicitly as INCOMPLETE_RECOVERED.
        If False, strictly raises json.JSONDecodeError on malformed files.

    Returns
    -------
    tuple[list[dict], dict]
        A two-element tuple:
        - **sessions** — list of raw session dicts (one per charging session).
        - **metadata** — the ``_meta`` dict from the API response plus an
          ``_extraction_status`` block detailing file validity, date range,
          and completeness.

    Raises
    ------
    FileNotFoundError
        If *source_path* does not exist.
    json.JSONDecodeError
        If the file fails strict parsing when allow_recovery=False, or if
        repair fails.
    """
    path = Path(source_path or DEFAULT_SOURCE).resolve()
    logger.info("Extract: reading %s (allow_recovery=%s)", path, allow_recovery)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    file_size = path.stat().st_size
    logger.info("Extract: file size = %s bytes (%.1f KB)", f"{file_size:,}", file_size / 1024)

    raw_text = path.read_text(encoding="utf-8")

    strict_parse_success = False
    recovery_mode_used = False
    parse_error_msg: str | None = None
    data = None

    try:
        data = json.loads(raw_text)
        strict_parse_success = True
    except json.JSONDecodeError as exc:
        parse_error_msg = (
            f"{exc.msg} at line {exc.lineno} column {exc.colno} (char {exc.pos})"
        )
        if not allow_recovery:
            logger.error("Extract: strict parse failed: %s", parse_error_msg)
            raise

        logger.warning(
            "Extract: strict parse failed (%s). Attempting in-memory recovery...",
            parse_error_msg,
        )
        repaired_text, appended_bytes = _repair_truncated_json(raw_text)
        data = json.loads(repaired_text)
        recovery_mode_used = True
        logger.warning(
            "Extract: in-memory recovery succeeded (+%d bytes). "
            "WARNING: Dataset is truncated and incomplete!",
            appended_bytes,
        )

    # Handle both root structures: dict with _items, or bare list
    if isinstance(data, dict):
        metadata = dict(data.get("_meta", {}))
        sessions = data.get("_items", [])
    elif isinstance(data, list):
        metadata = {}
        sessions = data
    else:
        raise ValueError(f"Unexpected JSON root type: {type(data).__name__}")

    # Evaluate date range completeness
    date_range_info = _check_date_range(sessions, metadata)

    # Overall completeness requires both strict parse success AND complete date range
    is_complete = strict_parse_success and date_range_info["date_range_complete"]
    status_label = "COMPLETE" if is_complete else "INCOMPLETE_RECOVERED" if recovery_mode_used else "INCOMPLETE_RANGE"

    extraction_status = {
        "status": status_label,
        "is_complete": is_complete,
        "strict_parse_success": strict_parse_success,
        "recovery_mode_used": recovery_mode_used,
        "parse_error": parse_error_msg,
        "recovered_records": len(sessions),
        "source_file": str(path),
        "source_size_bytes": file_size,
        "date_range": date_range_info,
    }

    metadata["_extraction_status"] = extraction_status

    logger.info(
        "Extract: loaded %d sessions from site '%s' (status: %s, date_range_complete: %s)",
        len(sessions),
        metadata.get("site", "unknown"),
        status_label,
        date_range_info["date_range_complete"],
    )

    return sessions, metadata


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    sessions, meta = extract_sessions()
    print(f"Extracted {len(sessions)} sessions")
    print(f"Metadata: {json.dumps(meta, indent=2, default=str)}")

