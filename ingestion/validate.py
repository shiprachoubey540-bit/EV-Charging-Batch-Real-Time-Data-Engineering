"""
ingestion/validate.py — Validate raw session records.

Owner: Ananthan S

This module checks each raw session record for required fields, correct
data types, parsable timestamps, and logical consistency.  Invalid records
are **flagged**, not dropped.
"""

import logging
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

REQUIRED_FIELDS: list[str] = [
    "_id",
    "sessionID",
    "connectionTime",
    "disconnectTime",
    "kWhDelivered",
    "stationID",
    "siteID",
]

TIMESTAMP_FIELDS: list[str] = [
    "connectionTime",
    "disconnectTime",
    "doneChargingTime",
]

USER_INPUT_NUMERIC_FIELDS: list[str] = [
    "WhPerMile",
    "kWhRequested",
    "milesRequested",
    "minutesAvailable",
]


def _parse_rfc2822(value: str | None) -> object | None:
    """
    Try to parse an RFC 2822 timestamp string.

    Returns the ``datetime`` on success or ``None`` on failure / null input.
    """
    if value is None:
        return None
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None


def validate_sessions(
    sessions: list[dict],
) -> tuple[list[dict], dict]:
    """
    Validate raw session records and tag problems.

    Each record receives a ``_validation_flags`` key (a list of strings).
    An empty list means the record passed all checks.

    Deduplication Policy
    --------------------
    Detection tags duplicates with ``duplicate_record_id`` and
    ``duplicate_session_id`` flags without silently discarding rows.
    For downstream analytics (Spark/Postgres), the recommended deduplication
    policy is to retain the first occurrence by (connection_time, ingestion_timestamp)
    or filter out rows where duplicate flags are present.

    Parameters
    ----------
    sessions : list[dict]
        Raw session records from :func:`extract.extract_sessions`.

    Returns
    -------
    tuple[list[dict], dict]
        - The same list of dicts, each augmented with ``_validation_flags``.
        - A summary dict with counts of each flag type.
    """
    flag_summary: dict[str, int] = {}
    seen_record_ids: set[str] = set()
    seen_session_ids: set[str] = set()

    def _add_flag(rec: dict, flag: str) -> None:
        rec["_validation_flags"].append(flag)
        flag_summary[flag] = flag_summary.get(flag, 0) + 1

    for idx, rec in enumerate(sessions):
        rec["_validation_flags"] = []

        # --- Duplicate identification ---
        rec_id = rec.get("_id")
        if rec_id is not None:
            if rec_id in seen_record_ids:
                _add_flag(rec, "duplicate_record_id")
            else:
                seen_record_ids.add(rec_id)

        session_id = rec.get("sessionID")
        if session_id is not None:
            if session_id in seen_session_ids:
                _add_flag(rec, "duplicate_session_id")
            else:
                seen_session_ids.add(session_id)

        # --- Required field check (null or empty string) ---
        for field in REQUIRED_FIELDS:
            val = rec.get(field)
            if val is None or (isinstance(val, str) and not val.strip()):
                _add_flag(rec, f"missing_required_field:{field}")

        # --- kWhDelivered type and range ---
        kwh = rec.get("kWhDelivered")
        if kwh is not None:
            if not isinstance(kwh, (int, float)):
                _add_flag(rec, "kwh_delivered_not_numeric")
            elif kwh < 0:
                _add_flag(rec, "kwh_delivered_negative")

        # --- Timestamp parsability ---
        parsed_ts: dict[str, object | None] = {}
        for ts_field in TIMESTAMP_FIELDS:
            raw_val = rec.get(ts_field)
            if raw_val is not None:
                dt = _parse_rfc2822(raw_val)
                if dt is None:
                    _add_flag(rec, f"unparseable_timestamp:{ts_field}")
                parsed_ts[ts_field] = dt
            else:
                parsed_ts[ts_field] = None

        # --- Logical timestamp checks ---
        conn = parsed_ts.get("connectionTime")
        disc = parsed_ts.get("disconnectTime")
        done = parsed_ts.get("doneChargingTime")

        if conn and disc and conn > disc:
            _add_flag(rec, "connection_after_disconnect")

        if done and disc and done > disc:
            _add_flag(rec, "done_after_disconnect")
            diff_sec = (done - disc).total_seconds()
            if diff_sec > 60:
                _add_flag(rec, "done_after_disconnect_outlier")

        if rec.get("doneChargingTime") is None and rec.get("kWhDelivered", 0) > 0:
            _add_flag(rec, "null_done_charging_time_with_energy")

        # --- Nested userInputs validation ---
        ui_list = rec.get("userInputs")
        if ui_list is not None:
            if not isinstance(ui_list, list):
                _add_flag(rec, "malformed_user_inputs")
            else:
                for ui in ui_list:
                    if not isinstance(ui, dict):
                        _add_flag(rec, "malformed_user_input_entry")
                        continue

                    # Numeric checks and negative prevention
                    for num_field in USER_INPUT_NUMERIC_FIELDS:
                        val = ui.get(num_field)
                        if val is not None:
                            if not isinstance(val, (int, float)):
                                _add_flag(rec, f"user_input_not_numeric:{num_field}")
                            elif val < 0:
                                _add_flag(rec, f"user_input_negative:{num_field}")

                    # Timestamps in userInputs
                    for ui_ts_field in ["modifiedAt", "requestedDeparture"]:
                        raw_ts = ui.get(ui_ts_field)
                        if raw_ts is not None:
                            if _parse_rfc2822(raw_ts) is None:
                                _add_flag(rec, f"user_input_unparseable_timestamp:{ui_ts_field}")

    # --- Summary ---
    total_flagged = sum(1 for r in sessions if r["_validation_flags"])
    total_clean = len(sessions) - total_flagged
    duplicate_records_count = sum(
        1 for r in sessions if any(f.startswith("duplicate_") for f in r["_validation_flags"])
    )

    logger.info(
        "Validate: %d records processed — %d clean, %d flagged (duplicates: %d)",
        len(sessions),
        total_clean,
        total_flagged,
        duplicate_records_count,
    )
    if flag_summary:
        for flag, count in sorted(flag_summary.items()):
            logger.warning("  Flag '%s': %d records", flag, count)

    summary = {
        "total_records": len(sessions),
        "clean_records": total_clean,
        "flagged_records": total_flagged,
        "duplicate_records": duplicate_records_count,
        "flags": flag_summary,
    }
    return sessions, summary


if __name__ == "__main__":
    import json
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion.extract import extract_sessions

    sessions, _meta = extract_sessions()
    validated, summary = validate_sessions(sessions)
    print(json.dumps(summary, indent=2))
