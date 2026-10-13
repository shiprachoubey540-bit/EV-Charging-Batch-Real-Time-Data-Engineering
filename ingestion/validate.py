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

    def _add_flag(rec: dict, flag: str) -> None:
        rec["_validation_flags"].append(flag)
        flag_summary[flag] = flag_summary.get(flag, 0) + 1

    for idx, rec in enumerate(sessions):
        rec["_validation_flags"] = []

        # --- Required field check ---
        for field in REQUIRED_FIELDS:
            if field not in rec or rec[field] is None:
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

        if rec.get("doneChargingTime") is None and rec.get("kWhDelivered", 0) > 0:
            _add_flag(rec, "null_done_charging_time_with_energy")

    # --- Summary ---
    total_flagged = sum(1 for r in sessions if r["_validation_flags"])
    total_clean = len(sessions) - total_flagged

    logger.info(
        "Validate: %d records processed — %d clean, %d flagged",
        len(sessions),
        total_clean,
        total_flagged,
    )
    if flag_summary:
        for flag, count in sorted(flag_summary.items()):
            logger.warning("  Flag '%s': %d records", flag, count)

    summary = {
        "total_records": len(sessions),
        "clean_records": total_clean,
        "flagged_records": total_flagged,
        "flags": flag_summary,
    }
    return sessions, summary


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    from ingestion.extract import extract_sessions

    sessions, _meta = extract_sessions()
    validated, summary = validate_sessions(sessions)
    print(json.dumps(summary, indent=2))
