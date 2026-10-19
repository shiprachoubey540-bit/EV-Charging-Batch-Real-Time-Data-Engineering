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
    execution_time: str | datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform raw session records into the canonical output schema.

    Parameters
    ----------
    sessions : list[dict]
        Validated session records (each may contain ``_validation_flags``).
    execution_time : str | datetime | None
        Optional fixed execution timestamp for deterministic pipeline runs.
        Defaults to current UTC time.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        - **sessions_df** — Main sessions table (one row per session).
        - **user_inputs_df** — Normalised user inputs table (one row per
          user-input entry, linked by ``session_id``).
    """
    if execution_time is None:
        ingestion_ts = datetime.now(timezone.utc).isoformat()
    elif isinstance(execution_time, datetime):
        ingestion_ts = execution_time.astimezone(timezone.utc).isoformat()
    else:
        ingestion_ts = str(execution_time)

    transformed_rows: list[dict] = []
    user_input_rows: list[dict] = []

    for rec in sessions:
        # --- Parse timestamps ---
        conn_iso = _rfc2822_to_iso(rec.get("connectionTime"))
        disc_iso = _rfc2822_to_iso(rec.get("disconnectTime"))
        done_iso = _rfc2822_to_iso(rec.get("doneChargingTime"))

        # --- Compute durations safely ---
        session_dur = None
        if conn_iso and disc_iso:
            dur = _duration_minutes(conn_iso, disc_iso)
            if dur is not None and dur >= 0:
                session_dur = dur

        # Check if doneChargingTime occurs after disconnectTime
        done_after_disc = False
        if disc_iso and done_iso:
            try:
                dt_disc = datetime.fromisoformat(disc_iso)
                dt_done = datetime.fromisoformat(done_iso)
                if dt_done > dt_disc:
                    done_after_disc = True
            except Exception:
                pass

        charging_dur = None
        idle_dur = None

        if not done_after_disc:
            # Durations are only reliable when doneChargingTime <= disconnectTime
            if conn_iso and done_iso:
                c_dur = _duration_minutes(conn_iso, done_iso)
                if c_dur is not None and c_dur >= 0:
                    charging_dur = c_dur

            if done_iso and disc_iso:
                i_dur = _duration_minutes(done_iso, disc_iso)
                if i_dur is not None and i_dur >= 0:
                    idle_dur = i_dur
        # When done_after_disc is True:
        # idle_duration_min and charging_duration_min are set to None (null)
        # to prevent negative idle duration from contaminating analytical aggregations.

        # --- Validation flags ---
        flags = rec.get("_validation_flags", [])
        flags_str = ",".join(flags) if flags else None

        row = {
            "id": str(rec["_id"]) if rec.get("_id") is not None else None,
            "session_id": str(rec["sessionID"]) if rec.get("sessionID") is not None else None,
            "site_id": str(rec["siteID"]) if rec.get("siteID") is not None else None,
            "cluster_id": str(rec["clusterID"]) if rec.get("clusterID") is not None else None,
            "station_id": str(rec["stationID"]) if rec.get("stationID") is not None else None,
            "space_id": str(rec["spaceID"]) if rec.get("spaceID") is not None else None,
            "connection_time": conn_iso,
            "disconnect_time": disc_iso,
            "done_charging_time": done_iso,
            "kwh_delivered": float(rec["kWhDelivered"]) if rec.get("kWhDelivered") is not None else None,
            "user_id": str(rec["userID"]) if rec.get("userID") is not None else None,
            "timezone": str(rec["timezone"]) if rec.get("timezone") is not None else None,
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
                    "session_id": str(rec["sessionID"]) if rec.get("sessionID") is not None else None,
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

    # Ensure identifier columns preserve string types
    str_cols = ["id", "session_id", "site_id", "cluster_id", "station_id", "space_id", "user_id", "timezone"]
    for col in str_cols:
        if col in sessions_df.columns:
            sessions_df[col] = sessions_df[col].astype(object)

    logger.info(
        "Transform: produced %d session rows, %d user-input rows",
        len(sessions_df),
        len(user_inputs_df),
    )
    return sessions_df, user_inputs_df


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion.extract import extract_sessions
    from ingestion.validate import validate_sessions

    raw, _meta = extract_sessions()
    validated, _summary = validate_sessions(raw)
    sess_df, ui_df = transform_sessions(validated)
    print(sess_df.head())
    print(ui_df.head())
