"""
ingestion/extract.py — Download or load raw ACN-Data CSV files.

Owner: Ananthan S

This module handles the extraction phase of the batch ETL pipeline.
Field names and paths will be finalised after examining the actual
ACN-Data dataset.
"""

import os
from dotenv import load_dotenv

load_dotenv()

DATA_RAW_DIR = os.getenv("DATA_RAW_DIR", "./data/raw")
DATA_SAMPLE_DIR = os.getenv("DATA_SAMPLE_DIR", "./data/sample")


def extract_sessions(source_path: str | None = None) -> list[dict]:
    """
    Load charging session records from a CSV file.

    Parameters
    ----------
    source_path : str | None
        Path to the CSV file. Defaults to a sample file for local
        development when other modules are not yet available.

    Returns
    -------
    list[dict]
        A list of session records conforming to the shared data
        contract (see contracts/data_contracts.yaml).
    """
    # TODO: Implement after examining the ACN-Data CSV structure
    raise NotImplementedError("Implement after dataset inspection")


if __name__ == "__main__":
    sessions = extract_sessions()
    print(f"Extracted {len(sessions)} sessions")
