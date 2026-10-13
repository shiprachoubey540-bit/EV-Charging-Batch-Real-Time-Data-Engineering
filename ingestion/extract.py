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
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data/raw")
DATA_SAMPLE_DIR = os.getenv("DATA_SAMPLE_DIR", "./data/sample")
DEFAULT_SOURCE = os.path.join(DATA_RAW_DIR, "acndata_sessions.json")


def _repair_truncated_json(text: str) -> str:
    """
    Attempt to repair a truncated ACN-Data JSON export.

    The ACN-Data API sometimes produces files where the ``_items`` array
    is cut short, ending with a trailing comma instead of ``]}``.  This
    function strips that comma and appends the missing closing tokens.

    Parameters
    ----------
    text : str
        Raw file content.

    Returns
    -------
    str
        Repaired JSON string, or the original if it already parses.
    """
    # Fast path: try parsing as-is
    try:
        json.loads(text)
        return text
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
            logger.warning(
                "Source JSON was truncated (missing closing '%s'). "
                "Repaired automatically — %d bytes appended.",
                suffix,
                len(suffix),
            )
            return candidate
        except json.JSONDecodeError:
            continue

    # Give up and return the original so the caller gets the real error
    logger.error("Could not repair truncated JSON.")
    return text


def extract_sessions(
    source_path: str | None = None,
) -> tuple[list[dict], dict]:
    """
    Load charging session records from an ACN-Data JSON export.

    Parameters
    ----------
    source_path : str | None
        Path to the JSON file.  Defaults to ``data/raw/acndata_sessions.json``.

    Returns
    -------
    tuple[list[dict], dict]
        A two-element tuple:
        - **sessions** — list of raw session dicts (one per charging session).
        - **metadata** — the ``_meta`` dict from the API response (may be
          empty if not present).

    Raises
    ------
    FileNotFoundError
        If *source_path* does not exist.
    json.JSONDecodeError
        If the file cannot be parsed even after repair attempts.
    """
    path = Path(source_path or DEFAULT_SOURCE).resolve()
    logger.info("Extract: reading %s", path)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    file_size = path.stat().st_size
    logger.info("Extract: file size = %s bytes (%.1f KB)", f"{file_size:,}", file_size / 1024)

    raw_text = path.read_text(encoding="utf-8")
    repaired = _repair_truncated_json(raw_text)
    data = json.loads(repaired)

    # Handle both root structures: dict with _items, or bare list
    if isinstance(data, dict):
        metadata = data.get("_meta", {})
        sessions = data.get("_items", [])
    elif isinstance(data, list):
        metadata = {}
        sessions = data
    else:
        raise ValueError(f"Unexpected JSON root type: {type(data).__name__}")

    logger.info(
        "Extract: loaded %d sessions from site '%s' (meta range: %s to %s)",
        len(sessions),
        metadata.get("site", "unknown"),
        metadata.get("start", "N/A"),
        metadata.get("end", "N/A"),
    )
    return sessions, metadata


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    sessions, meta = extract_sessions()
    print(f"Extracted {len(sessions)} sessions")
    print(f"Metadata: {meta}")
