"""
ingestion/transform.py — Clean, validate, and reshape raw session data.

Owner: Ananthan S

This module converts raw ACN-Data session records into the canonical
output schema.  It parses RFC 2822 timestamps to ISO 8601, computes
derived duration fields, renames columns to snake_case, and normalises
the nested ``userInputs`` array into a separate table.
"""

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import pandas as pd

logger = logging.getLogger(__name__)


# ---------- Helpers ---------- #

def _rfc2822_to_iso(value: str | None) -> str | None:
    """Convert an RFC 2822 timestamp to ISO 8601 UTC string, or None."""
    if value is None:
        return None
    try:
        dt = parsedate_to_datetime(value)
        # Ensure UTC
        dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except (TypeError, ValueError, IndexError):
        logger.warning("Could not parse timestamp: %r", value)
        return None


def _duration_minutes(start_iso: str | None, end_iso: str | None) -> float | None:
    """
    Compute the duration in minutes between two ISO 8601 timestamps.

    Returns ``None`` if either input is ``None``.
    """
    if start_iso is None or end_iso is None:
        return None
    try:
        dt_start = datetime.fromisoformat(start_iso)
        dt_end = datetime.fromisoformat(end_iso)
        delta = (dt_end - dt_start).total_seconds() / 60.0
        return round(delta, 2)
    except (TypeError, ValueError):
        return None


# ---------- Main transform ---------- #

def transform_sessions(
    sessions: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform raw session records into the canonical output schema.

    Parameters
    ----------
    sessions : list[dict]
        Validated session records (each may contain ``_validation_flags``).

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        - **sessions_df** — Main sessions table (one row per session).
        - **user_inputs_df** — Normalised user inputs table (one row per
          user-input entry, linked by ``session_id``).
    """
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    transformed_rows: list[dict] = []
    user_input_rows: list[dict] = []

    for rec in sessions:
        # --- Parse timestamps ---
        conn_iso = _rfc2822_to_iso(rec.get("connectionTime"))
        disc_iso = _rfc2822_to_iso(rec.get("disconnectTime"))
        done_iso = _rfc2822_to_iso(rec.get("doneChargingTime"))

        # --- Compute durations ---
        session_dur = _duration_minutes(conn_iso, disc_iso)
        charging_dur = _duration_minutes(conn_iso, done_iso)
        idle_dur = _duration_minutes(done_iso, disc_iso)

        # --- Validation flags ---
        flags = rec.get("_validation_flags", [])
        flags_str = ",".join(flags) if flags else None

        row = {
            "id": rec.get("_id"),
            "session_id": rec.get("sessionID"),
            "site_id": rec.get("siteID"),
            "cluster_id": rec.get("clusterID"),
            "station_id": rec.get("stationID"),
            "space_id": rec.get("spaceID"),
            "connection_time": conn_iso,
            "disconnect_time": disc_iso,
            "done_charging_time": done_iso,
            "kwh_delivered": rec.get("kWhDelivered"),
            "user_id": rec.get("userID"),
            "timezone": rec.get("timezone"),
            "session_duration_min": session_dur,
            "charging_duration_min": charging_dur,
            "idle_duration_min": idle_dur,
            "validation_flags": flags_str,
            "ingestion_timestamp": ingestion_ts,
        }
        transformed_rows.append(row)

        # --- Flatten userInputs ---
        ui_list = rec.get("userInputs")
        if ui_list and isinstance(ui_list, list):
            for ui in ui_list:
                if not isinstance(ui, dict):
                    continue
                ui_row = {
                    "session_id": rec.get("sessionID"),
                    "wh_per_mile": ui.get("WhPerMile"),
                    "kwh_requested": ui.get("kWhRequested"),
                    "miles_requested": ui.get("milesRequested"),
                    "minutes_available": ui.get("minutesAvailable"),
                    "modified_at": _rfc2822_to_iso(ui.get("modifiedAt")),
                    "payment_required": ui.get("paymentRequired"),
                    "requested_departure": _rfc2822_to_iso(
                        ui.get("requestedDeparture")
                    ),
                    "user_id_numeric": ui.get("userID"),
                }
                user_input_rows.append(ui_row)

    sessions_df = pd.DataFrame(transformed_rows)
    user_inputs_df = pd.DataFrame(user_input_rows)

    logger.info(
        "Transform: produced %d session rows, %d user-input rows",
        len(sessions_df),
        len(user_inputs_df),
    )
    return sessions_df, user_inputs_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion.extract import extract_sessions
    from ingestion.validate import validate_sessions

    raw, _meta = extract_sessions()
    validated, _summary = validate_sessions(raw)
    sess_df, ui_df = transform_sessions(validated)
    print(sess_df.head())
    print(ui_df.head())
