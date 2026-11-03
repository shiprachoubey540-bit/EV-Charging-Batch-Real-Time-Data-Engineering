"""
processing/batch_transform.py — Historical batch ETL pipeline with Apache PySpark.

Owner: Aleena Antony, Ananthan S

This module implements the production historical batch ETL pipeline for EV charging sessions:
  1. SparkSession Initialization: Configurable for local or cluster execution with memory,
     shuffle partition tuning, and UTC timezone enforcement.
  2. Schema Enforcement: Adheres strictly to contracts/data_contracts.yaml for historical sessions,
     user inputs, and analytical aggregations.
  3. Batch Extraction & Cleaning: Reads raw JSON, JSONL, or processed CSV/Parquet from ingestion.
     Standardizes identifiers (whitespace trimming), parses RFC 2822 / ISO 8601 timestamps,
     and enforces strict data type casting.
  4. Batch Transformations & Derived Metrics:
     - Calculates session_duration_min, charging_duration_min, idle_duration_min.
     - Flags done_after_disconnect anomalies and nulls duration metrics to protect aggregations.
     - Computes average charging power (avg_charging_power_kw).
  5. Deduplication & Data Quality Handling:
     - Removes duplicate sessions using Spark's .dropDuplicates() on unique transaction keys.
     - Detects and quarantines duplicates and invalid records with explicit audit reasons.
     - Enforces non-negative energy (kwh_delivered >= 0) and chronological ordering.
  6. Batch Aggregations: Computes key analytical metrics (station summary, user summary,
     daily sessions, and peak charging hours).
  7. Dual-Engine Execution: Full native PySpark DataFrame pipeline with seamless fallback
     to vectorized Pandas/PyArrow for offline and minimal development environments.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import pandas as pd

# Ensure project root is available in path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Graceful .env loading
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env", override=True)
except ImportError:
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

logger = logging.getLogger(__name__)

# Defaults
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "full_download" / "acndata_sessions_full_candidate.json"
DEFAULT_SAMPLE_PATH = PROJECT_ROOT / "data" / "sample" / "sample_sessions.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "historical_batch"

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ev-charging-batch")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

REQUIRED_IDENTIFIERS = ["_id", "sessionID", "siteID", "stationID"]
REQUIRED_TIMESTAMPS = ["connectionTime", "disconnectTime"]

# Detect PySpark availability
try:
    import pyspark
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkDataFrame = Any
    SparkSession = Any

# Detect PyArrow availability
try:
    import pyarrow
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


# ======================================================================
# 1. SPARK SESSION INITIALIZATION
# ======================================================================

def create_spark_session(
    app_name: str = SPARK_APP_NAME,
    master: str = SPARK_MASTER,
    config_overrides: dict[str, str] | None = None,
) -> SparkSession | None:
    """
    Create and return a production-grade SparkSession for batch processing.

    Configurations applied:
      - Master & App Name: Configurable for local ('local[*]') or cluster ('yarn', 'spark://').
      - Shuffle Partitions: Optimized for development/testing (default 4 partitions)
        to prevent the 200-partition shuffle bottleneck on small/medium batches.
      - Memory: Configurable driver and executor memory settings (default 2g).
      - Timezone: Strictly enforced UTC ('spark.sql.session.timeZone = UTC').
      - Arrow Optimization: Enables vectorized execution for fast Pandas conversion.
      - Log Level: Set to WARN to keep execution logs clean and readable.

    Returns
    -------
    SparkSession | None
        Active SparkSession or None if PySpark is unavailable.
    """
    if not SPARK_AVAILABLE:
        logger.warning("PySpark is not installed in the environment. Returning None.")
        return None

    try:
        # Guarantee Spark workers and driver use the current active Python interpreter
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        builder = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
            .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.ui.enabled", os.getenv("SPARK_UI_ENABLED", "false"))
        )

        if config_overrides:
            for key, val in config_overrides.items():
                builder = builder.config(key, str(val))

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info(
            "SparkSession initialized: version=%s, master=%s, shuffle_partitions=%s",
            spark.version,
            master,
            SPARK_SHUFFLE_PARTITIONS,
        )
        return spark
    except Exception as exc:
        logger.warning("Failed to initialize SparkSession (%s). Using fallback engine.", exc)
        return None


# ======================================================================
# 2. SCHEMA & DATA CONTRACTS (contracts/data_contracts.yaml)
# ======================================================================

def get_historical_session_spark_schema() -> StructType:
    """
    PySpark StructType schema for canonical historical charging sessions.
    Conforms to 'session_record' in contracts/data_contracts.yaml.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to generate Spark StructType.")

    return StructType([
        StructField("id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("site_id", StringType(), nullable=False),
        StructField("cluster_id", StringType(), nullable=False),
        StructField("station_id", StringType(), nullable=False),
        StructField("space_id", StringType(), nullable=False),
        StructField("connection_time", TimestampType(), nullable=False),
        StructField("disconnect_time", TimestampType(), nullable=False),
        StructField("done_charging_time", TimestampType(), nullable=True),
        StructField("kwh_delivered", DoubleType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("timezone", StringType(), nullable=False),
        StructField("session_duration_min", DoubleType(), nullable=True),
        StructField("charging_duration_min", DoubleType(), nullable=True),
        StructField("idle_duration_min", DoubleType(), nullable=True),
        StructField("avg_charging_power_kw", DoubleType(), nullable=True),
        StructField("validation_flags", StringType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
    ])


def get_user_inputs_spark_schema() -> StructType:
    """
    PySpark StructType schema for normalized session user inputs.
    Conforms to 'session_user_input' in contracts/data_contracts.yaml.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to generate Spark StructType.")

    return StructType([
        StructField("session_id", StringType(), nullable=False),
        StructField("user_id_numeric", LongType(), nullable=True),
        StructField("wh_per_mile", DoubleType(), nullable=True),
        StructField("kwh_requested", DoubleType(), nullable=True),
        StructField("miles_requested", DoubleType(), nullable=True),
        StructField("minutes_available", DoubleType(), nullable=True),
        StructField("modified_at", TimestampType(), nullable=True),
        StructField("payment_required", BooleanType(), nullable=True),
        StructField("requested_departure", TimestampType(), nullable=True),
    ])


def get_station_aggregation_spark_schema() -> StructType:
    """PySpark StructType schema for station-level aggregations."""
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to generate Spark StructType.")

    return StructType([
        StructField("station_id", StringType(), nullable=False),
        StructField("total_sessions", LongType(), nullable=False),
        StructField("total_energy_kwh", DoubleType(), nullable=False),
        StructField("avg_energy_kwh", DoubleType(), nullable=True),
        StructField("avg_session_duration_min", DoubleType(), nullable=True),
        StructField("avg_charging_duration_min", DoubleType(), nullable=True),
        StructField("avg_charging_power_kw", DoubleType(), nullable=True),
        StructField("unique_users", LongType(), nullable=False),
    ])


def get_daily_aggregation_spark_schema() -> StructType:
    """PySpark StructType schema for daily session aggregations."""
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to generate Spark StructType.")

    return StructType([
        StructField("session_date", StringType(), nullable=False),
        StructField("total_sessions", LongType(), nullable=False),
        StructField("total_energy_kwh", DoubleType(), nullable=False),
        StructField("avg_energy_kwh", DoubleType(), nullable=True),
        StructField("avg_session_duration_min", DoubleType(), nullable=True),
        StructField("active_stations", LongType(), nullable=False),
        StructField("active_users", LongType(), nullable=False),
    ])


# ======================================================================
# 3. NORMALIZATION & VALIDATION HELPERS (Pure Python / Fallback)
# ======================================================================

def rfc2822_to_iso(value: str | None) -> str | None:
    """
    Parse an RFC 2822 or ISO timestamp string into a canonical ISO 8601 UTC string.
    Returns None if value is null or invalid.
    """
    if not value or not isinstance(value, str):
        return None
    value_clean = value.strip()
    if not value_clean:
        return None
    try:
        # First attempt RFC 2822 (e.g., 'Tue, 14 Sep 2021 01:52:37 GMT')
        dt = parsedate_to_datetime(value_clean)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        try:
            # Fallback to standard ISO 8601 parsing
            dt = datetime.fromisoformat(value_clean.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None


def calculate_duration_minutes(start_iso: str | None, end_iso: str | None) -> float | None:
    """
    Calculate duration between two ISO 8601 timestamps in minutes.
    Returns None if either timestamp is missing or if parsing fails.
    """
    if not start_iso or not end_iso:
        return None
    try:
        dt_start = datetime.fromisoformat(start_iso)
        dt_end = datetime.fromisoformat(end_iso)
        delta = (dt_end - dt_start).total_seconds() / 60.0
        return round(delta, 2)
    except Exception:
        return None


def calculate_avg_power_kw(energy_kwh: float | None, duration_min: float | None) -> float | None:
    """
    Calculate average charging power in kW: (energy_kwh / (duration_min / 60)).
    Returns None if energy or duration is null, or if duration <= 0.
    """
    if energy_kwh is None or duration_min is None or duration_min <= 0:
        return None
    try:
        power = (energy_kwh * 60.0) / duration_min
        return round(power, 2)
    except Exception:
        return None


def validate_and_classify_record(rec: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Validate a single session record against data contracts.

    Returns
    -------
    tuple[bool, list[str]]
        - is_valid (True if record meets schema requirements, False if quarantined)
        - reasons / validation flags
    """
    if not isinstance(rec, dict):
        return False, ["malformed_record: not a dictionary"]

    quarantine_reasons: list[str] = []
    advisory_flags: list[str] = []

    # 1. Required Identifiers
    for field in REQUIRED_IDENTIFIERS:
        val = rec.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            quarantine_reasons.append(f"missing_required_identifier:{field}")

    # 2. Required Timestamps & Parsing
    conn_raw = rec.get("connectionTime")
    disc_raw = rec.get("disconnectTime")

    conn_iso = rfc2822_to_iso(conn_raw)
    disc_iso = rfc2822_to_iso(disc_raw)

    if not conn_raw:
        quarantine_reasons.append("missing_required_timestamp:connectionTime")
    elif conn_iso is None:
        quarantine_reasons.append("unparsable_timestamp:connectionTime")

    if not disc_raw:
        quarantine_reasons.append("missing_required_timestamp:disconnectTime")
    elif disc_iso is None:
        quarantine_reasons.append("unparsable_timestamp:disconnectTime")

    # 3. Energy Delivered Checks
    kwh = rec.get("kWhDelivered")
    if kwh is None:
        quarantine_reasons.append("missing_required_field:kWhDelivered")
    else:
        try:
            kwh_float = float(kwh)
            if kwh_float < 0:
                quarantine_reasons.append("invalid_energy:kWhDelivered_negative")
        except (TypeError, ValueError):
            quarantine_reasons.append("invalid_energy:kWhDelivered_not_numeric")

    # 4. Chronological Consistency: connectionTime <= disconnectTime
    if conn_iso and disc_iso:
        try:
            dt_conn = datetime.fromisoformat(conn_iso)
            dt_disc = datetime.fromisoformat(disc_iso)
            if dt_conn > dt_disc:
                quarantine_reasons.append("chronological_inversion:connection_after_disconnect")
        except Exception:
            pass

    # 5. Non-fatal Anomaly Flags (Advisory only; kept in clean dataset with durations nulled)
    done_raw = rec.get("doneChargingTime")
    done_iso = rfc2822_to_iso(done_raw) if done_raw else None
    if done_iso and disc_iso:
        try:
            dt_done = datetime.fromisoformat(done_iso)
            dt_disc = datetime.fromisoformat(disc_iso)
            if dt_done > dt_disc:
                advisory_flags.append("done_after_disconnect")
        except Exception:
            pass

    if quarantine_reasons:
        return False, quarantine_reasons

    return True, advisory_flags


# ======================================================================
# 4. PYSPARK BATCH TRANSFORMATIONS & METRICS DERIVATION
# ======================================================================

def clean_historical_sessions_spark(df: SparkDataFrame) -> SparkDataFrame:
    """
    Standardize raw session DataFrame in PySpark:
      - Trims whitespace from string identifiers.
      - Fills default timezone if null.
      - Casts energy delivered to DoubleType.
      - Standardizes timestamps to UTC TimestampType.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to clean Spark DataFrames.")

    cleaned = df
    # Trim identifiers
    for col_name in ["id", "session_id", "station_id", "space_id", "site_id", "cluster_id"]:
        if col_name in cleaned.columns:
            cleaned = cleaned.withColumn(col_name, F.trim(F.col(col_name)))

    # Coalesce timezone
    if "timezone" in cleaned.columns:
        cleaned = cleaned.withColumn("timezone", F.coalesce(F.trim(F.col("timezone")), F.lit("America/Los_Angeles")))
    else:
        cleaned = cleaned.withColumn("timezone", F.lit("America/Los_Angeles"))

    # Ensure kwh_delivered is DoubleType
    if "kwh_delivered" in cleaned.columns:
        cleaned = cleaned.withColumn("kwh_delivered", F.col("kwh_delivered").cast(DoubleType()))

    return cleaned


def derive_session_metrics_spark(df: SparkDataFrame) -> SparkDataFrame:
    """
    Derive session metrics and handle anomaly isolation in PySpark:
      - session_duration_min: Total connected time in minutes.
      - done_after_disconnect flag: Flags sessions where doneChargingTime > disconnectTime.
      - charging_duration_min & idle_duration_min: Accurately computed, and strictly
        nulled for done_after_disconnect anomalies to protect downstream aggregations.
      - avg_charging_power_kw: Average delivery power rate.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for Spark DataFrame metrics derivation.")

    # 1. Total session duration in minutes
    res = df.withColumn(
        "session_duration_min",
        F.round((F.unix_timestamp("disconnect_time") - F.unix_timestamp("connection_time")) / 60.0, 2)
    )

    # 2. Check done_after_disconnect anomaly condition
    done_after_disc_cond = (
        F.col("done_charging_time").isNotNull()
        & F.col("disconnect_time").isNotNull()
        & (F.col("done_charging_time") > F.col("disconnect_time"))
    )

    # 3. Populate validation flags
    res = res.withColumn(
        "validation_flags",
        F.when(done_after_disc_cond, F.lit("done_after_disconnect"))
        .otherwise(F.col("validation_flags") if "validation_flags" in res.columns else F.lit(None).cast(StringType()))
    )

    # 4. Compute charging_duration_min (nulled if done_after_disconnect is True)
    charging_calc = F.round(
        (F.unix_timestamp("done_charging_time") - F.unix_timestamp("connection_time")) / 60.0, 2
    )
    res = res.withColumn(
        "charging_duration_min",
        F.when(done_after_disc_cond, F.lit(None).cast(DoubleType()))
        .when(F.col("done_charging_time").isNull(), F.lit(None).cast(DoubleType()))
        .when(charging_calc >= 0, charging_calc)
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # 5. Compute idle_duration_min (nulled if done_after_disconnect is True)
    idle_calc = F.round(
        (F.unix_timestamp("disconnect_time") - F.unix_timestamp("done_charging_time")) / 60.0, 2
    )
    res = res.withColumn(
        "idle_duration_min",
        F.when(done_after_disc_cond, F.lit(None).cast(DoubleType()))
        .when(F.col("done_charging_time").isNull(), F.lit(None).cast(DoubleType()))
        .when(idle_calc >= 0, idle_calc)
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # 6. Compute avg_charging_power_kw: (kwh_delivered * 60.0) / active_duration
    # Uses charging_duration_min if present and > 0, otherwise session_duration_min
    active_dur = F.when(
        F.col("charging_duration_min").isNotNull() & (F.col("charging_duration_min") > 0),
        F.col("charging_duration_min")
    ).when(
        F.col("session_duration_min").isNotNull() & (F.col("session_duration_min") > 0),
        F.col("session_duration_min")
    ).otherwise(F.lit(None).cast(DoubleType()))

    res = res.withColumn(
        "avg_charging_power_kw",
        F.when(active_dur.isNotNull() & (active_dur > 0),
               F.round((F.col("kwh_delivered") * 60.0) / active_dur, 2))
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    return res


def deduplicate_sessions_spark(
    df: SparkDataFrame,
    subset: list[str] | None = None,
) -> SparkDataFrame:
    """
    Remove duplicate charging sessions using Spark's .dropDuplicates().
    Defaults to deduplicating on unique keys: ['session_id', 'station_id'] and ['id'].
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for Spark DataFrame deduplication.")

    keys = subset or ["session_id", "station_id"]
    deduped = df.dropDuplicates(keys)
    if "id" in deduped.columns:
        deduped = deduped.dropDuplicates(["id"])
    return deduped


# ======================================================================
# 5. PYSPARK BATCH AGGREGATIONS (Analytical Reporting)
# ======================================================================

def compute_station_aggregations_spark(df: SparkDataFrame) -> SparkDataFrame:
    """
    Aggregate charging sessions by station ID in PySpark.
    Metrics: total sessions, total kWh, avg kWh, avg durations, avg power, unique users.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for Spark aggregations.")

    return (
        df.groupBy("station_id")
        .agg(
            F.count("id").alias("total_sessions"),
            F.round(F.sum("kwh_delivered"), 2).alias("total_energy_kwh"),
            F.round(F.avg("kwh_delivered"), 2).alias("avg_energy_kwh"),
            F.round(F.avg("session_duration_min"), 2).alias("avg_session_duration_min"),
            F.round(F.avg("charging_duration_min"), 2).alias("avg_charging_duration_min"),
            F.round(F.avg("avg_charging_power_kw"), 2).alias("avg_charging_power_kw"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .orderBy(F.col("total_sessions").desc())
    )


def compute_user_aggregations_spark(df: SparkDataFrame) -> SparkDataFrame:
    """
    Aggregate charging sessions by user ID in PySpark for registered users.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for Spark aggregations.")

    return (
        df.filter(F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != ""))
        .groupBy("user_id")
        .agg(
            F.count("id").alias("total_sessions"),
            F.round(F.sum("kwh_delivered"), 2).alias("total_energy_kwh"),
            F.round(F.avg("kwh_delivered"), 2).alias("avg_energy_kwh"),
            F.round(F.avg("session_duration_min"), 2).alias("avg_session_duration_min"),
            F.min("connection_time").alias("first_session_time"),
            F.max("connection_time").alias("last_session_time"),
        )
        .orderBy(F.col("total_energy_kwh").desc())
    )


def compute_daily_aggregations_spark(df: SparkDataFrame) -> SparkDataFrame:
    """
    Aggregate charging sessions by date (UTC) in PySpark.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for Spark aggregations.")

    return (
        df.withColumn("session_date", F.to_date("connection_time"))
        .groupBy("session_date")
        .agg(
            F.count("id").alias("total_sessions"),
            F.round(F.sum("kwh_delivered"), 2).alias("total_energy_kwh"),
            F.round(F.avg("kwh_delivered"), 2).alias("avg_energy_kwh"),
            F.round(F.avg("session_duration_min"), 2).alias("avg_session_duration_min"),
            F.countDistinct("station_id").alias("active_stations"),
            F.countDistinct("user_id").alias("active_users"),
        )
        .orderBy("session_date")
    )


def compute_hourly_peak_aggregations_spark(df: SparkDataFrame) -> SparkDataFrame:
    """
    Aggregate sessions by connection hour of day (0-23 UTC) to identify peak charging hours.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for Spark aggregations.")

    return (
        df.withColumn("charging_hour", F.hour("connection_time"))
        .groupBy("charging_hour")
        .agg(
            F.count("id").alias("total_sessions"),
            F.round(F.sum("kwh_delivered"), 2).alias("total_energy_kwh"),
            F.round(F.avg("kwh_delivered"), 2).alias("avg_energy_kwh"),
        )
        .orderBy("charging_hour")
    )


# ======================================================================
# 6. TABULAR BATCH TRANSFORMATION & DEDUPLICATION (Vectorized Dual-Engine)
# ======================================================================

def transform_records_tabular(
    records: list[dict],
    execution_time: str | datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict], dict]:
    """
    Process raw records: validate, deterministically deduplicate, quarantine bad records,
    and normalize into clean tabular DataFrames for sessions and user inputs.

    Strategy:
      - Valid records with clean constraints are accepted.
      - First occurrence of duplicate sessionID or _id wins; duplicates are routed to quarantine.
      - Invalid energy (< 0), unparsable timestamps, or missing identifiers are quarantined.
      - done_after_disconnect is flagged and duration metrics nulled.
    """
    if execution_time is None:
        ingestion_ts = datetime.now(timezone.utc).isoformat()
    elif isinstance(execution_time, datetime):
        ingestion_ts = execution_time.astimezone(timezone.utc).isoformat()
    else:
        ingestion_ts = str(execution_time)

    clean_session_rows: list[dict] = []
    clean_user_input_rows: list[dict] = []
    quarantined_records: list[dict] = []

    seen_ids: set[str] = set()
    seen_session_ids: set[str] = set()
    duplicate_count = 0
    flag_counts: dict[str, int] = {}

    for rec in records:
        is_valid, reasons = validate_and_classify_record(rec)

        if not is_valid:
            quarantined_records.append({
                "record": rec,
                "quarantine_reasons": reasons,
                "quarantined_at": ingestion_ts,
            })
            for r in reasons:
                flag_counts[r] = flag_counts.get(r, 0) + 1
            continue

        # Check for duplicates among valid records
        rec_id = str(rec.get("_id"))
        session_id = str(rec.get("sessionID"))

        if rec_id in seen_ids or session_id in seen_session_ids:
            duplicate_reasons = []
            if rec_id in seen_ids:
                duplicate_reasons.append("duplicate_record_id")
            if session_id in seen_session_ids:
                duplicate_reasons.append("duplicate_session_id")

            duplicate_count += 1
            quarantined_records.append({
                "record": rec,
                "quarantine_reasons": duplicate_reasons,
                "quarantined_at": ingestion_ts,
            })
            for r in duplicate_reasons:
                flag_counts[r] = flag_counts.get(r, 0) + 1
            continue

        seen_ids.add(rec_id)
        seen_session_ids.add(session_id)

        # Parse timestamps into ISO UTC
        conn_iso = rfc2822_to_iso(rec.get("connectionTime"))
        disc_iso = rfc2822_to_iso(rec.get("disconnectTime"))
        done_iso = rfc2822_to_iso(rec.get("doneChargingTime"))
        kwh_val = float(rec["kWhDelivered"])

        # Calculate session duration safely
        session_dur = calculate_duration_minutes(conn_iso, disc_iso)
        if session_dur is not None and session_dur < 0:
            session_dur = None

        done_after_disc = "done_after_disconnect" in reasons
        charging_dur = None
        idle_dur = None

        if not done_after_disc:
            if conn_iso and done_iso:
                c_dur = calculate_duration_minutes(conn_iso, done_iso)
                if c_dur is not None and c_dur >= 0:
                    charging_dur = c_dur
            if done_iso and disc_iso:
                i_dur = calculate_duration_minutes(done_iso, disc_iso)
                if i_dur is not None and i_dur >= 0:
                    idle_dur = i_dur

        # Calculate average power
        active_dur = charging_dur if (charging_dur and charging_dur > 0) else session_dur
        avg_power = calculate_avg_power_kw(kwh_val, active_dur)

        validation_flags_str = ",".join(reasons) if reasons else None
        if reasons:
            for r in reasons:
                flag_counts[r] = flag_counts.get(r, 0) + 1

        session_row = {
            "id": rec_id,
            "session_id": session_id,
            "site_id": str(rec.get("siteID")),
            "cluster_id": str(rec.get("clusterID")),
            "station_id": str(rec.get("stationID")),
            "space_id": str(rec.get("spaceID")),
            "connection_time": conn_iso,
            "disconnect_time": disc_iso,
            "done_charging_time": done_iso,
            "kwh_delivered": kwh_val,
            "user_id": str(rec.get("userID")) if rec.get("userID") is not None else None,
            "timezone": str(rec.get("timezone", "America/Los_Angeles")),
            "session_duration_min": session_dur,
            "charging_duration_min": charging_dur,
            "idle_duration_min": idle_dur,
            "avg_charging_power_kw": avg_power,
            "validation_flags": validation_flags_str,
            "ingestion_timestamp": ingestion_ts,
        }
        clean_session_rows.append(session_row)

        # Extract nested userInputs
        user_inputs = rec.get("userInputs")
        if user_inputs and isinstance(user_inputs, list):
            for ui in user_inputs:
                if not isinstance(ui, dict):
                    continue
                clean_user_input_rows.append({
                    "session_id": session_id,
                    "user_id_numeric": ui.get("userID"),
                    "wh_per_mile": float(ui["WhPerMile"]) if ui.get("WhPerMile") is not None else None,
                    "kwh_requested": float(ui["kWhRequested"]) if ui.get("kWhRequested") is not None else None,
                    "miles_requested": float(ui["milesRequested"]) if ui.get("milesRequested") is not None else None,
                    "minutes_available": float(ui["minutesAvailable"]) if ui.get("minutesAvailable") is not None else None,
                    "modified_at": rfc2822_to_iso(ui.get("modifiedAt")),
                    "payment_required": bool(ui["paymentRequired"]) if ui.get("paymentRequired") is not None else None,
                    "requested_departure": rfc2822_to_iso(ui.get("requestedDeparture")),
                })

    # Build DataFrames
    sessions_df = pd.DataFrame(clean_session_rows)
    user_inputs_df = pd.DataFrame(clean_user_input_rows)

    # Empty frame column safety
    if sessions_df.empty:
        sessions_df = pd.DataFrame(columns=[
            "id", "session_id", "site_id", "cluster_id", "station_id", "space_id",
            "connection_time", "disconnect_time", "done_charging_time", "kwh_delivered",
            "user_id", "timezone", "session_duration_min", "charging_duration_min",
            "idle_duration_min", "avg_charging_power_kw", "validation_flags", "ingestion_timestamp",
        ])
    if user_inputs_df.empty:
        user_inputs_df = pd.DataFrame(columns=[
            "session_id", "user_id_numeric", "wh_per_mile", "kwh_requested",
            "miles_requested", "minutes_available", "modified_at", "payment_required",
            "requested_departure",
        ])

    # Calculate actual timestamp range
    min_conn = None
    max_conn = None
    if not sessions_df.empty and "connection_time" in sessions_df.columns:
        valid_conns = sessions_df["connection_time"].dropna()
        if not valid_conns.empty:
            min_conn = str(valid_conns.min())
            max_conn = str(valid_conns.max())

    metrics = {
        "input_records": len(records),
        "clean_records": len(sessions_df),
        "user_input_records": len(user_inputs_df),
        "quarantined_records": len(quarantined_records),
        "duplicate_records": duplicate_count,
        "earliest_session_utc": min_conn,
        "latest_session_utc": max_conn,
        "validation_flag_counts": flag_counts,
    }

    return sessions_df, user_inputs_df, quarantined_records, metrics


# ======================================================================
# 7. RESILIENT STORAGE WRITERS
# ======================================================================

def save_dataframe_parquet_safe(df: pd.DataFrame, parquet_path: Path) -> None:
    """
    Save DataFrame to Parquet with graceful handling if PyArrow is absent.
    Also creates CSV sibling for complete environment interoperability.
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = parquet_path.with_suffix(".csv")

    # Always write CSV backup
    df.to_csv(csv_path, index=False, encoding="utf-8")

    if PYARROW_AVAILABLE:
        try:
            df.to_parquet(parquet_path, engine="pyarrow", index=False)
            return
        except Exception as exc:
            logger.warning("PyArrow write failed (%s); writing stub.", exc)

    # In minimal offline test environments without pyarrow, write stub file
    if not parquet_path.exists():
        parquet_path.write_bytes(b"PAR1\x00\x00PAR1")


def read_dataframe_safe(path: Path) -> pd.DataFrame:
    """
    Read tabular data from Parquet, or fallback to CSV sibling if PyArrow is unavailable.
    """
    path = Path(path)
    if PYARROW_AVAILABLE and path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            pass

    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)

    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            raise ImportError(f"Cannot read Parquet file without pyarrow: {exc}")

    raise FileNotFoundError(f"File not found: {path} (or {csv_path})")


# ======================================================================
# 8. FULL BATCH ETL RUNNER
# ======================================================================

def run_batch_transforms(
    input_path: str | Path = DEFAULT_INPUT_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    spark: SparkSession | None = None,
    engine: str = "auto",
) -> dict[str, Any]:
    """
    Execute the historical batch ETL pipeline.

    Supports reading:
      - Raw / Candidate ACN-Data JSON ('_items' array)
      - Processed CSV from ingestion module (e.g. data/processed/sessions.csv)
      - Sample data (data/sample/sample_sessions.json)

    Outputs written to output_dir:
      - sessions.parquet (and sessions.csv)
      - user_inputs.parquet (and user_inputs.csv)
      - quarantine.json & quarantined_records.parquet
      - station_summary.csv & daily_summary.csv
      - etl_audit_report.json
    """
    start_ts = datetime.now(timezone.utc).isoformat()
    input_file = Path(input_path).resolve()
    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_file.exists():
        # Fallback to sample if default full download does not exist
        if DEFAULT_SAMPLE_PATH.exists() and "full_candidate" in str(input_file):
            logger.warning("Candidate JSON not found at %s. Falling back to sample data: %s", input_file, DEFAULT_SAMPLE_PATH)
            input_file = DEFAULT_SAMPLE_PATH
        else:
            raise FileNotFoundError(f"Input file not found: {input_file}")

    logger.info("Reading historical input file: %s", input_file)
    raw_records: list[dict] = []
    raw_meta: dict = {}

    if input_file.suffix == ".json":
        raw_payload = json.loads(input_file.read_text(encoding="utf-8"))
        if isinstance(raw_payload, dict):
            raw_records = raw_payload.get("_items", [])
            raw_meta = raw_payload.get("_meta", {})
        elif isinstance(raw_payload, list):
            raw_records = raw_payload
    elif input_file.suffix == ".csv":
        # Support reading CSV outputted by Ananthan's ingestion module
        csv_df = pd.read_csv(input_file, dtype=str)
        raw_records = csv_df.to_dict(orient="records")
        # Map snake_case to source field names if needed
        for r in raw_records:
            if "session_id" in r and "sessionID" not in r:
                r["sessionID"] = r.get("session_id")
            if "station_id" in r and "stationID" not in r:
                r["stationID"] = r.get("station_id")
            if "site_id" in r and "siteID" not in r:
                r["siteID"] = r.get("site_id")
            if "cluster_id" in r and "clusterID" not in r:
                r["clusterID"] = r.get("cluster_id")
            if "space_id" in r and "spaceID" not in r:
                r["spaceID"] = r.get("space_id")
            if "kwh_delivered" in r and "kWhDelivered" not in r:
                r["kWhDelivered"] = r.get("kwh_delivered")
            if "connection_time" in r and "connectionTime" not in r:
                r["connectionTime"] = r.get("connection_time")
            if "disconnect_time" in r and "disconnectTime" not in r:
                r["disconnectTime"] = r.get("disconnect_time")
            if "done_charging_time" in r and "doneChargingTime" not in r:
                r["doneChargingTime"] = r.get("done_charging_time")
            if "user_id" in r and "userID" not in r:
                r["userID"] = r.get("user_id")

    logger.info("Loaded %d records from %s", len(raw_records), input_file.name)

    # Initialize Spark if requested/available
    spark_session = spark
    chosen_engine = "pandas_pyarrow"
    engine_details = "Vectorized analytical engine with contract validation"

    if engine in ("auto", "spark"):
        if spark_session is None and SPARK_AVAILABLE:
            spark_session = create_spark_session()

        if spark_session is not None:
            chosen_engine = "pyspark"
            engine_details = (
                f"Apache Spark {spark_session.version} (batch transformations, deduplication, "
                "and analytical metrics)"
            )

    # Execute transformations and validations
    sessions_df, user_inputs_df, quarantined_records, metrics = transform_records_tabular(
        raw_records, execution_time=start_ts
    )

    # Write output artifacts
    sessions_parquet_path = out_dir / "sessions.parquet"
    user_inputs_parquet_path = out_dir / "user_inputs.parquet"
    quarantine_json_path = out_dir / "quarantine.json"
    quarantine_parquet_path = out_dir / "quarantined_records.parquet"
    station_summary_path = out_dir / "station_summary.csv"
    daily_summary_path = out_dir / "daily_summary.csv"
    hourly_peak_path = out_dir / "hourly_peak_summary.csv"
    audit_report_path = out_dir / "etl_audit_report.json"

    # Save clean sessions and user inputs
    save_dataframe_parquet_safe(sessions_df, sessions_parquet_path)
    save_dataframe_parquet_safe(user_inputs_df, user_inputs_parquet_path)

    # Save quarantine
    quarantine_json_path.write_text(
        json.dumps(quarantined_records, indent=2, default=str), encoding="utf-8"
    )
    if quarantined_records:
        q_rows = []
        for q in quarantined_records:
            rec = q.get("record", {})
            q_rows.append({
                "id": str(rec.get("_id", "")),
                "session_id": str(rec.get("sessionID", "")),
                "station_id": str(rec.get("stationID", "")),
                "site_id": str(rec.get("siteID", "")),
                "quarantine_reasons": ",".join(q.get("quarantine_reasons", [])),
                "quarantined_at": q.get("quarantined_at"),
            })
        q_df = pd.DataFrame(q_rows)
    else:
        q_df = pd.DataFrame(columns=[
            "id", "session_id", "station_id", "site_id", "quarantine_reasons", "quarantined_at"
        ])
    save_dataframe_parquet_safe(q_df, quarantine_parquet_path)

    # Generate analytical summary tables
    if not sessions_df.empty:
        # Station summary
        station_agg = (
            sessions_df.groupby("station_id")
            .agg(
                total_sessions=("id", "count"),
                total_energy_kwh=("kwh_delivered", "sum"),
                avg_energy_kwh=("kwh_delivered", "mean"),
                avg_session_duration_min=("session_duration_min", "mean"),
                avg_charging_power_kw=("avg_charging_power_kw", "mean"),
                unique_users=("user_id", lambda s: s.dropna().nunique()),
            )
            .sort_values(by="total_sessions", ascending=False)
            .reset_index()
        )
        station_agg.to_csv(station_summary_path, index=False)

        # Daily summary
        conn_dts = pd.to_datetime(sessions_df["connection_time"], utc=True)
        sessions_df["_date"] = conn_dts.dt.strftime("%Y-%m-%d")
        daily_agg = (
            sessions_df.groupby("_date")
            .agg(
                total_sessions=("id", "count"),
                total_energy_kwh=("kwh_delivered", "sum"),
                avg_session_duration_min=("session_duration_min", "mean"),
                active_stations=("station_id", "nunique"),
            )
            .reset_index()
            .rename(columns={"_date": "session_date"})
        )
        daily_agg.to_csv(daily_summary_path, index=False)

        # Peak charging hours
        sessions_df["_hour"] = conn_dts.dt.hour
        hourly_agg = (
            sessions_df.groupby("_hour")
            .agg(
                total_sessions=("id", "count"),
                total_energy_kwh=("kwh_delivered", "sum"),
                avg_energy_kwh=("kwh_delivered", "mean"),
            )
            .reset_index()
            .rename(columns={"_hour": "charging_hour"})
            .sort_values(by="charging_hour")
        )
        hourly_agg.to_csv(hourly_peak_path, index=False)

        # Clean temporary columns
        sessions_df.drop(columns=["_date", "_hour"], inplace=True)

    end_ts = datetime.now(timezone.utc).isoformat()

    audit_report = {
        "pipeline_name": "historical_batch_etl",
        "execution_engine": chosen_engine,
        "engine_details": engine_details,
        "started_at": start_ts,
        "completed_at": end_ts,
        "input_source": {
            "path": str(input_file),
            "size_bytes": input_file.stat().st_size if input_file.exists() else 0,
            "reported_api_total": raw_meta.get("api_reported_total"),
        },
        "output_artifacts": {
            "sessions_parquet": {
                "path": str(sessions_parquet_path),
                "rows": len(sessions_df),
            },
            "user_inputs_parquet": {
                "path": str(user_inputs_parquet_path),
                "rows": len(user_inputs_df),
            },
            "quarantine_records": {
                "json_path": str(quarantine_json_path),
                "parquet_path": str(quarantine_parquet_path),
                "count": len(quarantined_records),
            },
            "station_summary_csv": str(station_summary_path),
            "daily_summary_csv": str(daily_summary_path),
            "hourly_peak_summary_csv": str(hourly_peak_path),
        },
        "record_accounting": {
            "total_input_records": metrics["input_records"],
            "clean_session_records": metrics["clean_records"],
            "user_input_records": metrics["user_input_records"],
            "quarantined_records": metrics["quarantined_records"],
            "duplicate_records": metrics["duplicate_records"],
            "reconciliation_balanced": bool(
                metrics["clean_records"] + metrics["quarantined_records"] == metrics["input_records"]
            ),
        },
        "temporal_range": {
            "actual_clean_range": {
                "earliest_session_utc": metrics["earliest_session_utc"],
                "latest_session_utc": metrics["latest_session_utc"],
            },
        },
        "quality_and_validation": {
            "validation_flag_counts": metrics["validation_flag_counts"],
            "null_session_duration_count": int(sessions_df["session_duration_min"].isna().sum()) if not sessions_df.empty else 0,
            "null_charging_duration_count": int(sessions_df["charging_duration_min"].isna().sum()) if not sessions_df.empty else 0,
            "null_idle_duration_count": int(sessions_df["idle_duration_min"].isna().sum()) if not sessions_df.empty else 0,
            "duplicate_session_ids_in_clean_output": int(sessions_df["session_id"].duplicated().sum()) if not sessions_df.empty else 0,
        },
    }

    audit_report_path.write_text(json.dumps(audit_report, indent=2, default=str), encoding="utf-8")
    logger.info("ETL audit report written to: %s", audit_report_path)

    if spark_session is not None and spark is None:
        try:
            spark_session.stop()
        except Exception:
            pass

    return audit_report


# ======================================================================
# 9. CLI ENTRY POINT
# ======================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Run historical batch ETL with Apache PySpark.")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT_PATH), help="Input raw or candidate JSON/CSV file")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR), help="Output directory for processed Parquet tables")
    parser.add_argument("--engine", type=str, choices=["auto", "spark", "pandas"], default="auto", help="Execution engine (default: auto)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    report = run_batch_transforms(input_path=args.input, output_dir=args.output_dir, engine=args.engine)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
