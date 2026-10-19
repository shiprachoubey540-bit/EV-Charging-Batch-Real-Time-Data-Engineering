"""
ingestion — EV Charging Sessions ETL pipeline.

Owner: Ananthan S

This package implements a four-stage batch ingestion pipeline for
ACN-Data EV charging sessions:

    Extract  → read raw JSON from ``data/raw/``
    Validate → flag anomalous / malformed records
    Transform → reshape, parse timestamps, normalise nested fields
    Load     → write cleaned CSV, JSONL, and quality reports to ``data/processed/``

Quick start
-----------
    >>> from ingestion import run_pipeline
    >>> results = run_pipeline()

Individual stages can also be imported directly:

    >>> from ingestion import extract_sessions, validate_sessions
    >>> from ingestion import transform_sessions, load_sessions
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.extract import extract_sessions
from ingestion.validate import validate_sessions
from ingestion.transform import transform_sessions
from ingestion.load import load_sessions
from ingestion.run_pipeline import run_pipeline

__all__ = [
    "extract_sessions",
    "validate_sessions",
    "transform_sessions",
    "load_sessions",
    "run_pipeline",
]
