"""
ingestion/transform.py — Clean, validate, and reshape raw session data.

Owner: Ananthan S
"""


def clean_sessions(raw_sessions: list[dict]) -> list[dict]:
    """
    Apply cleaning rules to raw session records.

    Parameters
    ----------
    raw_sessions : list[dict]
        Raw records from extract.py.

    Returns
    -------
    list[dict]
        Cleaned records ready for loading.
    """
    # TODO: Implement cleaning logic
    raise NotImplementedError("Implement cleaning rules")
