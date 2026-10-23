"""
dashboard/components/data_loader.py — Multi-tiered Data Access Layer for Streamlit.

Owner: Shipra Choubey

Architecture:
  Tier 1: Live PostgreSQL database (when configured via env / st.secrets)
  Tier 2: Historical Batch Parquet tables (data/processed/historical_batch/)
  Tier 3: Processed CSV files (data/processed/)
  Tier 4: Sample JSON fallback (data/sample/)

Includes caching (@st.cache_data), date/time normalization to UTC & America/Los_Angeles,
quality audit loading, and non-destructive database health checks.
"""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import zoneinfo

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
HISTORICAL_BATCH_DIR = PROCESSED_DIR / "historical_batch"
VALIDATION_DIR = PROCESSED_DIR / "validation"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"

SITE_TIMEZONE = "America/Los_Angeles"


def get_postgres_connection_params() -> Optional[dict[str, Any]]:
    """
    Retrieve PostgreSQL connection parameters from Streamlit secrets or OS environment.
    Never exposes or logs secrets.
    """
    # 1. Safely check Streamlit secrets
    try:
        if hasattr(st, "secrets") and "postgres" in st.secrets:
            return dict(st.secrets["postgres"])
    except Exception:
        pass

    # 2. Check environment variables
    host = os.getenv("POSTGRES_HOST")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT", "5432")

    if host and db and user and pwd:
        return {
            "host": host,
            "port": int(port),
            "dbname": db,
            "user": user,
            "password": pwd,
        }
    return None


def check_postgres_status() -> dict[str, Any]:
    """
    Test live PostgreSQL connectivity without raising unhandled errors.
    """
    params = get_postgres_connection_params()
    if not params:
        return {
            "configured": False,
            "connected": False,
            "reason": "Credentials not configured in .env or st.secrets",
        }

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=params.get("host"),
            port=params.get("port", 5432),
            dbname=params.get("dbname"),
            user=params.get("user"),
            password=params.get("password"),
            connect_timeout=2,
        )
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'charging_sessions';")
        has_table = cur.fetchone()[0] > 0
        cur.close()
        conn.close()
        return {
            "configured": True,
            "connected": True,
            "has_table": has_table,
            "reason": "Connection established successfully",
        }
    except Exception as exc:
        return {
            "configured": True,
            "connected": False,
            "reason": f"Connection refused or failed: {type(exc).__name__}",
        }


@st.cache_data(ttl=60, show_spinner=False)
def load_sessions_data() -> Tuple[pd.DataFrame, dict[str, Any]]:
    """
    Load charging sessions from the active tier (PostgreSQL -> Parquet -> CSV).

    Returns
    -------
    Tuple[pd.DataFrame, dict]
        Standardized DataFrame and metadata dictionary describing data origin.
    """
    df = pd.DataFrame()
    meta = {
        "tier": "None",
        "source_name": "No Data Found",
        "row_count": 0,
        "is_database": False,
    }

    # Tier 1: PostgreSQL
    pg_status = check_postgres_status()
    if pg_status["connected"] and pg_status.get("has_table"):
        try:
            params = get_postgres_connection_params()
            import psycopg2
            conn = psycopg2.connect(**params)
            query = "SELECT * FROM charging_sessions LIMIT 50000;"
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                meta = {
                    "tier": "PostgreSQL",
                    "source_name": f"PostgreSQL Table (charging_sessions) @ {params.get('host')}",
                    "row_count": len(df),
                    "is_database": True,
                }
        except Exception as exc:
            logger.warning("PostgreSQL query failed, falling back to local files: %s", exc)

    # Tier 2: Historical Batch Parquet
    parquet_path = HISTORICAL_BATCH_DIR / "sessions.parquet"
    if df.empty and parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            meta = {
                "tier": "Parquet",
                "source_name": f"Apache Parquet ({parquet_path.name})",
                "row_count": len(df),
                "is_database": False,
            }
        except Exception as exc:
            logger.warning("Parquet read failed: %s", exc)

    # Tier 3: Processed CSV
    csv_path = PROCESSED_DIR / "sessions.csv"
    if df.empty and csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            meta = {
                "tier": "CSV",
                "source_name": f"CSV Dataset ({csv_path.name})",
                "row_count": len(df),
                "is_database": False,
            }
        except Exception as exc:
            logger.error("CSV read failed: %s", exc)

    # Clean & normalize schema
    if not df.empty:
        # Standardize timestamps
        for ts_col in ["connection_time", "disconnect_time", "done_charging_time", "ingestion_timestamp"]:
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

        # Standardize numerics
        for num_col in ["kwh_delivered", "session_duration_min", "charging_duration_min", "idle_duration_min", "avg_charging_power_kw"]:
            if num_col in df.columns:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

        # Timezone-aware date & time features (America/Los_Angeles for Caltech site)
        if "connection_time" in df.columns:
            # Derived UTC attributes
            df["connection_date"] = df["connection_time"].dt.date
            df["connection_hour_utc"] = df["connection_time"].dt.hour
            df["day_name_utc"] = df["connection_time"].dt.day_name()

            # Derived Local Time attributes (Pacific Time)
            try:
                local_series = df["connection_time"].dt.tz_convert(SITE_TIMEZONE)
                df["connection_time_local"] = local_series
                df["connection_date_local"] = local_series.dt.date
                df["connection_hour_local"] = local_series.dt.hour
                df["day_name_local"] = local_series.dt.day_name()
            except Exception:
                df["connection_time_local"] = df["connection_time"]
                df["connection_date_local"] = df["connection_date"]
                df["connection_hour_local"] = df["connection_hour_utc"]
                df["day_name_local"] = df["day_name_utc"]

        # Quality flag normalization
        if "validation_flags" in df.columns:
            df["validation_flags"] = df["validation_flags"].fillna("").astype(str)
            df["is_clean"] = df["validation_flags"].str.strip() == ""
        else:
            df["is_clean"] = True
            df["validation_flags"] = ""

        meta["row_count"] = len(df)

    return df, meta


@st.cache_data(ttl=60, show_spinner=False)
def load_user_inputs_data() -> Tuple[pd.DataFrame, dict[str, Any]]:
    """
    Load normalized driver input preferences from the active tier.
    """
    df = pd.DataFrame()
    meta = {"source_name": "No Data Found", "row_count": 0}

    parquet_path = HISTORICAL_BATCH_DIR / "user_inputs.parquet"
    csv_path = PROCESSED_DIR / "user_inputs.csv"

    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            meta = {"source_name": f"Parquet ({parquet_path.name})", "row_count": len(df)}
        except Exception:
            pass

    if df.empty and csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            meta = {"source_name": f"CSV ({csv_path.name})", "row_count": len(df)}
        except Exception:
            pass

    if not df.empty:
        for col in ["wh_per_mile", "kwh_requested", "miles_requested", "minutes_available"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for ts_col in ["modified_at", "requested_departure"]:
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

    return df, meta


@st.cache_data(ttl=60, show_spinner=False)
def load_pipeline_audit_reports() -> dict[str, Any]:
    """
    Load data quality and batch pipeline execution audit logs.
    """
    results: dict[str, Any] = {
        "verification_report": None,
        "quality_report": None,
        "download_report": None,
        "pipeline_log_tail": [],
    }

    # 1. Primary verification report
    v_path = VALIDATION_DIR / "verification_report.json"
    if v_path.exists():
        try:
            results["verification_report"] = json.loads(v_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 2. Ingestion quality report
    q_path = PROCESSED_DIR / "quality_report.json"
    if q_path.exists():
        try:
            results["quality_report"] = json.loads(q_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 3. Downloader audit report
    d_path = RAW_DIR / "full_download" / "download_report.json"
    if d_path.exists():
        try:
            results["download_report"] = json.loads(d_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 4. Pipeline log tail
    log_path = PROCESSED_DIR / "pipeline.log"
    if log_path.exists():
        try:
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            results["pipeline_log_tail"] = lines[-30:] if len(lines) > 30 else lines
        except Exception:
            pass

    return results

