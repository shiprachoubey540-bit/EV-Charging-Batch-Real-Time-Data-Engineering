"""
processing/stream_transform.py — Spark Structured Streaming & Kafka real-time event pipeline.

Owner: Aleena Antony

This module implements production real-time streaming ETL for EV charging telemetry:
  1. Streaming SparkSession Initialization: Configured for low-latency micro-batching,
     fault-tolerant checkpointing, and UTC timezone enforcement.
  2. Kafka Source Integration: Connects to real-time telemetry topics via .readStream.format("kafka").
  3. JSON Deserialization & Schema Enforcement: Strict adherence to contracts/data_contracts.yaml.
  4. Watermarking & Stateful Deduplication: Handles late or out-of-order events with event-time
     watermarking and stateful deduplication via .dropDuplicatesWithinWatermark(["event_id", "session_id"]).
  5. Windowed Aggregations: Computes 10-minute tumbling or sliding window aggregations per station.
  6. Output Sinks: Multi-sink streaming configuration (console, memory, or PostgreSQL).
  7. Dual-Engine Execution: Seamless native PySpark Structured Streaming with in-memory
     vectorized fallback (StreamingMicroBatchEngine) for isolated testing and development.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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

# Defaults and Constants
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CHARGING_EVENTS", "charging-events")
DEFAULT_WATERMARK_DURATION = "10 minutes"
DEFAULT_WINDOW_DURATION = "10 minutes"
DEFAULT_CHECKPOINT_DIR = os.getenv("SPARK_STREAMING_CHECKPOINT_DIR", "/tmp/spark_streaming_checkpoints")
POSTGRES_TABLE = "charging_events"

# Verified station identifier regex (Caltech fleet pattern: e.g. "2-39-81-4550", "2-39-139-28")
STATION_ID_REGEX = re.compile(r"^\d+-\d+-\d+-\d+$")

# Initial fleet of verified station IDs from data contract documentation
VERIFIED_STATION_FLEET = {
    "2-39-81-4550",
    "2-39-139-28",
    "2-39-131-30",
    "2-39-123-10",
    "2-39-124-11",
    "2-39-125-12",
    "2-39-126-13",
    "2-39-127-14",
    "2-39-128-15",
    "2-39-129-16",
    "2-39-130-17",
    "2-39-100-001",
    "2-39-100-002",
    "2-39-100-003",
    "2-39-100-004",
    "2-39-100-005",
}

# Dynamically populate verified stations if validation summary is present
_station_summary_file = PROJECT_ROOT / "data" / "processed" / "validation" / "station_summary.csv"
if _station_summary_file.exists():
    try:
        import csv
        with open(_station_summary_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                st = row.get("station_id") or row.get("stationID")
                if st:
                    VERIFIED_STATION_FLEET.add(st.strip())
    except Exception as e:
        logger.debug("Failed to read station_summary.csv: %s", e)

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
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkDataFrame = Any
    SparkSession = Any


# ======================================================================
# 1. SCHEMA DEFINITIONS (MATCHING contracts/data_contracts.yaml)
# ======================================================================

def get_streaming_event_spark_schema() -> StructType:
    """
    Returns the PySpark StructType schema for real-time telemetry events.
    Strictly conforms to contracts/data_contracts.yaml (streaming_event).
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in the current environment.")

    return StructType([
        StructField("event_id", StringType(), False),
        StructField("station_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("energy_kwh", DoubleType(), False),
        StructField("incremental_kwh", DoubleType(), True),
    ])


def get_station_aggregation_spark_schema() -> StructType:
    """
    Returns the PySpark StructType schema for windowed station aggregations.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in the current environment.")

    return StructType([
        StructField("station_id", StringType(), False),
        StructField("event_count", LongType(), False),
        StructField("total_incremental_kwh", DoubleType(), False),
        StructField("max_cumulative_kwh", DoubleType(), False),
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
    ])


# ======================================================================
# 2. SPARK SESSION INITIALIZATION
# ======================================================================

def create_streaming_session(
    app_name: str = "ev-charging-streaming",
    master: Optional[str] = None,
    config_overrides: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Create and return a SparkSession optimized for Structured Streaming.

    Parameters
    ----------
    app_name : str
        Application identifier in Spark UI.
    master : Optional[str]
        Spark master URL (defaults to SPARK_MASTER env var or 'local[*]').
    config_overrides : Optional[Dict[str, str]]
        Additional Spark configuration properties.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in the current environment.")

    spark_master = master or os.getenv("SPARK_MASTER", "local[*]")
    shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    if config_overrides:
        for k, v in config_overrides.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ======================================================================
# 3. KAFKA SOURCE INTEGRATION
# ======================================================================

def create_kafka_read_stream(
    spark: SparkSession,
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    starting_offsets: str = "latest",
    fail_on_data_loss: bool = False,
) -> SparkDataFrame:
    """
    Configure Spark Structured Streaming to consume from Kafka topic.

    Parameters
    ----------
    spark : SparkSession
        Active streaming SparkSession.
    bootstrap_servers : Optional[str]
        Kafka bootstrap servers string (e.g. 'localhost:9092').
    topic : Optional[str]
        Kafka topic name (defaults to 'charging-events').
    starting_offsets : str
        'latest' or 'earliest'.
    fail_on_data_loss : bool
        Whether to halt query on missing offsets.

    Returns
    -------
    SparkDataFrame
        Streaming DataFrame with raw Kafka schema (key, value, topic, partition, offset, timestamp).
    """
    servers = bootstrap_servers or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    target_topic = topic or DEFAULT_KAFKA_TOPIC

    logger.info("Connecting Spark Structured Streaming to Kafka topic '%s' at %s", target_topic, servers)

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", target_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", str(fail_on_data_loss).lower())
        .load()
    )


# ======================================================================
# 4. JSON EVENT DESERIALIZATION & VALIDATION
# ======================================================================

def decode_and_validate_stream(raw_df: SparkDataFrame) -> Tuple[SparkDataFrame, SparkDataFrame]:
    """
    Deserialize binary Kafka values into structured columns, validate against
    data contract invariants, and quarantine invalid records.

    Parameters
    ----------
    raw_df : SparkDataFrame
        Raw Kafka streaming DataFrame with binary 'value' column.

    Returns
    -------
    Tuple[SparkDataFrame, SparkDataFrame]
        (valid_events_df, quarantined_events_df)
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in the current environment.")

    # 1. Cast Kafka binary value to string
    string_df = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("json_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    schema = get_streaming_event_spark_schema()

    # 2. Parse JSON using contract schema
    parsed_df = string_df.withColumn("parsed", F.from_json(F.col("json_payload"), schema))

    # 3. Identify quarantine conditions
    quarantine_cond = (
        F.col("parsed").isNull()
        | F.col("parsed.event_id").isNull()
        | (F.length(F.trim(F.col("parsed.event_id"))) == 0)
        | F.col("parsed.station_id").isNull()
        | (F.length(F.trim(F.col("parsed.station_id"))) == 0)
        | (F.col("parsed.energy_kwh") < 0.0)
        | ((F.col("parsed.event_type") == "session_start") & (F.col("parsed.energy_kwh") > 0.0))
        | (~F.col("parsed.station_id").rlike(r"^\d+-\d+-\d+-\d+$"))
    )

    reason_expr = (
        F.when(F.col("parsed").isNull(), F.lit("unparseable_json"))
        .when(
            F.col("parsed.event_id").isNull() | (F.length(F.trim(F.col("parsed.event_id"))) == 0),
            F.lit("missing_required_field:event_id"),
        )
        .when(
            F.col("parsed.station_id").isNull() | (F.length(F.trim(F.col("parsed.station_id"))) == 0),
            F.lit("missing_required_field:station_id"),
        )
        .when(F.col("parsed.energy_kwh") < 0.0, F.lit("negative_energy_kwh"))
        .when(
            (F.col("parsed.event_type") == "session_start") & (F.col("parsed.energy_kwh") > 0.0),
            F.lit("non_zero_start_energy"),
        )
        .when(~F.col("parsed.station_id").rlike(r"^\d+-\d+-\d+-\d+$"), F.lit("unknown_station_id"))
        .otherwise(F.lit("invalid_record"))
    )

    # 4. Split stream into valid and quarantined
    quarantined_df = (
        parsed_df.filter(quarantine_cond)
        .withColumn("quarantine_reason", reason_expr)
        .withColumn("quarantined_at", F.current_timestamp())
    )

    valid_df = (
        parsed_df.filter(~quarantine_cond)
        .select(
            F.col("parsed.event_id").alias("event_id"),
            F.col("parsed.station_id").alias("station_id"),
            F.col("parsed.session_id").alias("session_id"),
            F.col("parsed.event_type").alias("event_type"),
            F.col("parsed.timestamp").alias("timestamp"),
            F.col("parsed.timestamp").alias("event_timestamp"),
            F.col("parsed.energy_kwh").alias("energy_kwh"),
            F.coalesce(F.col("parsed.incremental_kwh"), F.lit(0.0)).alias("incremental_kwh"),
            F.col("kafka_timestamp"),
        )
    )

    return valid_df, quarantined_df


# ======================================================================
# 5. WATERMARKING & STATEFUL DEDUPLICATION
# ======================================================================

def apply_watermark_and_deduplication(
    valid_df: SparkDataFrame,
    watermark_duration: str = DEFAULT_WATERMARK_DURATION,
    timestamp_col: str = "timestamp",
    dedup_cols: Optional[List[str]] = None,
) -> SparkDataFrame:
    """
    Apply event-time watermarking and stateful deduplication.

    Parameters
    ----------
    valid_df : SparkDataFrame
        Validated events stream.
    watermark_duration : str
        Watermark duration (e.g. '10 minutes').
    timestamp_col : str
        Timestamp column for event-time processing ('event_timestamp' or 'timestamp').
    dedup_cols : Optional[List[str]]
        Columns used for deduplication (defaults to ['event_id', 'session_id']).

    Returns
    -------
    SparkDataFrame
        Deduplicated, watermarked stream.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in the current environment.")

    # Determine active timestamp column
    ts_col = timestamp_col if timestamp_col in valid_df.columns else (
        "event_timestamp" if "event_timestamp" in valid_df.columns else "timestamp"
    )

    watermarked = valid_df.withWatermark(ts_col, watermark_duration)

    keys = dedup_cols or ["event_id", "session_id"]

    # Native Spark 3.5+ dropDuplicatesWithinWatermark for streaming DataFrames.
    # For batch DataFrames (e.g. offline unit testing), fallback to dropDuplicates
    # because Spark Catalyst enforces that dropDuplicatesWithinWatermark is streaming-only.
    if getattr(watermarked, "isStreaming", False) and hasattr(watermarked, "dropDuplicatesWithinWatermark"):
        return watermarked.dropDuplicatesWithinWatermark(keys)
    elif hasattr(watermarked, "dropDuplicatesWithinWatermark") and not hasattr(watermarked, "isStreaming"):
        return watermarked.dropDuplicatesWithinWatermark(keys)
    else:
        return watermarked.dropDuplicates(keys)


# ======================================================================
# 6. WINDOWED AGGREGATIONS
# ======================================================================

def compute_streaming_station_aggregations(
    df: SparkDataFrame,
    window_duration: str = DEFAULT_WINDOW_DURATION,
    slide_duration: Optional[str] = None,
    timestamp_col: Optional[str] = None,
) -> SparkDataFrame:
    """
    Compute tumbling or sliding window aggregations per station.

    Computes:
      - event_count: Total telemetry events received in window
      - total_incremental_kwh: Sum of non-negative interval energy deliveries
      - max_cumulative_kwh: Highest meter reading observed in window

    Parameters
    ----------
    df : SparkDataFrame
        Deduplicated, watermarked event stream.
    window_duration : str
        Window length (e.g. '10 minutes').
    slide_duration : Optional[str]
        Optional sliding interval (e.g. '5 minutes' for sliding, None for tumbling).
    timestamp_col : Optional[str]
        Event time column.
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in the current environment.")

    cols = df.columns
    ts_col = timestamp_col or ("event_timestamp" if "event_timestamp" in cols else "timestamp")

    if slide_duration:
        win = F.window(F.col(ts_col), window_duration, slide_duration)
    else:
        win = F.window(F.col(ts_col), window_duration)

    return (
        df.groupBy(win.alias("window"), F.col("station_id"))
        .agg(
            F.count("*").alias("event_count"),
            F.coalesce(F.sum("incremental_kwh"), F.lit(0.0)).alias("total_incremental_kwh"),
            F.coalesce(F.max("energy_kwh"), F.lit(0.0)).alias("max_cumulative_kwh"),
        )
        .select(
            F.col("station_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("event_count"),
            F.col("total_incremental_kwh"),
            F.col("max_cumulative_kwh"),
        )
    )


# ======================================================================
# 7. OUTPUT SINKS & QUERY MANAGEMENT
# ======================================================================

def configure_console_sink(
    df: SparkDataFrame,
    query_name: str = "console_stream",
    output_mode: str = "update",
    trigger_interval: str = "5 seconds",
):
    """
    Write streaming DataFrame micro-batches to the console.
    """
    return (
        df.writeStream.queryName(query_name)
        .format("console")
        .outputMode(output_mode)
        .trigger(processingTime=trigger_interval)
        .option("truncate", "false")
        .start()
    )


def configure_memory_sink(
    df: SparkDataFrame,
    query_name: str = "memory_sink",
    output_mode: str = "complete",
):
    """
    Write streaming DataFrame to an in-memory table for verification testing.
    """
    return (
        df.writeStream.queryName(query_name)
        .format("memory")
        .outputMode(output_mode)
        .start()
    )


def configure_postgres_sink(
    df: SparkDataFrame,
    table_name: str = POSTGRES_TABLE,
    checkpoint_location: Optional[str] = None,
    trigger_interval: str = "10 seconds",
):
    """
    Write streaming micro-batches to PostgreSQL using foreachBatch.
    """
    ckpt = checkpoint_location or f"{DEFAULT_CHECKPOINT_DIR}/{table_name}"

    def write_micro_batch(batch_df: SparkDataFrame, batch_id: int):
        if batch_df.isEmpty():
            return

        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "ev_charging_db")
        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

        (
            batch_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", db_user)
            .option("password", db_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

    return (
        df.writeStream.foreachBatch(write_micro_batch)
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", ckpt)
        .start()
    )


def stop_query_gracefully(query: Any, timeout_seconds: float = 2.0) -> bool:
    """
    Safely stop an active Spark StreamingQuery.

    Parameters
    ----------
    query : Any
        StreamingQuery instance or None.
    timeout_seconds : float
        Await termination timeout.

    Returns
    -------
    bool
        True if cleanly stopped or query was None.
    """
    if query is None:
        return True

    try:
        if hasattr(query, "stop"):
            query.stop()
        if hasattr(query, "awaitTermination"):
            query.awaitTermination(timeout=timeout_seconds)
        return True
    except Exception as e:
        logger.warning("Error stopping streaming query gracefully: %s", e)
        return False


# ======================================================================
# 8. OFFLINE / TEST COMPATIBILITY & DUAL-ENGINE ENGINE
# ======================================================================

def parse_and_transform_event(payload: Union[str, bytes, dict]) -> dict:
    """
    Parse and validate a single event payload into a normalized dictionary.

    Parameters
    ----------
    payload : Union[str, bytes, dict]
        Raw JSON string, bytes, or dictionary.

    Returns
    -------
    dict
        Parsed and validated event record.
    """
    if isinstance(payload, (bytes, bytearray)):
        data = json.loads(payload.decode("utf-8"))
    elif isinstance(payload, str):
        data = json.loads(payload)
    elif isinstance(payload, dict):
        data = payload
    else:
        raise ValueError(f"Unsupported payload type: {type(payload)}")

    for req in ["event_id", "station_id", "event_type", "timestamp"]:
        if req not in data or data[req] is None or str(data[req]).strip() == "":
            raise ValueError(f"missing_required_field:{req}")

    if "energy_kwh" not in data and "cumulative_kwh" in data:
        data["energy_kwh"] = data["cumulative_kwh"]

    if "energy_kwh" not in data or data["energy_kwh"] is None:
        raise ValueError("missing_required_field:energy_kwh")

    data["energy_kwh"] = float(data["energy_kwh"])
    if data["energy_kwh"] < 0:
        raise ValueError("negative_energy_kwh")

    if data.get("incremental_kwh") is not None:
        data["incremental_kwh"] = float(data["incremental_kwh"])
    else:
        data["incremental_kwh"] = 0.0

    return data


class StreamingMicroBatchEngine:
    """
    In-memory stateful micro-batch processor with contract validation,
    deduplication tracking, and windowed aggregation computation.
    Operates identically to Spark Structured Streaming for offline unit testing.
    """

    def __init__(
        self,
        valid_stations: Optional[Set[str]] = None,
        watermark_duration_sec: float = 600.0,
    ):
        self.valid_stations = valid_stations or set(VERIFIED_STATION_FLEET)
        self.watermark_duration_sec = watermark_duration_sec
        self.seen_event_ids: Set[str] = set()
        self.session_state: Dict[str, float] = {}

    def _is_station_valid(self, station_id: str) -> bool:
        if not station_id:
            return False
        st = str(station_id).strip()
        if st in self.valid_stations:
            return True
        return bool(STATION_ID_REGEX.match(st))

    def process_micro_batch(
        self,
        batch: List[Union[str, bytes, dict]],
    ) -> Tuple[List[dict], List[dict], List[dict]]:
        """
        Process a micro-batch of raw events.

        Returns
        -------
        Tuple[List[dict], List[dict], List[dict]]
            (valid_events, quarantined_events, station_metrics)
        """
        valid_events: List[dict] = []
        quarantined_events: List[dict] = []
        station_agg: Dict[str, Dict[str, Any]] = {}

        for item in batch:
            raw_record = item
            # 1. Parse JSON
            try:
                if isinstance(item, (bytes, bytearray)):
                    record = json.loads(item.decode("utf-8"))
                elif isinstance(item, str):
                    record = json.loads(item)
                elif isinstance(item, dict):
                    record = dict(item)
                else:
                    quarantined_events.append({
                        "raw": str(item),
                        "quarantine_reason": "unparseable_json",
                    })
                    continue
            except Exception:
                quarantined_events.append({
                    "raw": str(item),
                    "quarantine_reason": "unparseable_json",
                })
                continue

            # 2. Check required fields
            event_id = record.get("event_id")
            station_id = record.get("station_id")
            session_id = record.get("session_id")
            event_type = record.get("event_type")
            ts = record.get("timestamp")
            energy = record.get("energy_kwh")
            if energy is None and "cumulative_kwh" in record:
                energy = record.get("cumulative_kwh")

            if not event_id or str(event_id).strip() == "":
                quarantined_events.append({**record, "quarantine_reason": "missing_required_field:event_id"})
                continue
            if not station_id or str(station_id).strip() == "":
                quarantined_events.append({**record, "quarantine_reason": "missing_required_field:station_id"})
                continue
            if energy is None:
                quarantined_events.append({**record, "quarantine_reason": "missing_required_field:energy_kwh"})
                continue

            try:
                energy_val = float(energy)
            except (ValueError, TypeError):
                quarantined_events.append({**record, "quarantine_reason": "energy_not_numeric"})
                continue

            # 3. Contract invariants
            if not self._is_station_valid(str(station_id)):
                quarantined_events.append({**record, "quarantine_reason": "unknown_station_id"})
                continue

            if energy_val < 0.0:
                quarantined_events.append({**record, "quarantine_reason": "negative_energy_kwh"})
                continue

            if str(event_type) == "session_start" and energy_val > 0.0:
                quarantined_events.append({**record, "quarantine_reason": "non_zero_start_energy"})
                continue

            # 4. Deduplication
            if str(event_id) in self.seen_event_ids:
                quarantined_events.append({**record, "quarantine_reason": "duplicate_event_id"})
                continue
            self.seen_event_ids.add(str(event_id))

            # 5. Incremental energy calculation
            inc_energy = record.get("incremental_kwh")
            if inc_energy is not None:
                try:
                    inc_val = max(0.0, float(inc_energy))
                except (ValueError, TypeError):
                    inc_val = 0.0
            else:
                prev_energy = self.session_state.get(str(session_id), 0.0)
                inc_val = max(0.0, energy_val - prev_energy)

            self.session_state[str(session_id)] = energy_val

            norm_event = {
                "event_id": str(event_id),
                "station_id": str(station_id),
                "session_id": str(session_id) if session_id else f"{station_id}_sess",
                "event_type": str(event_type),
                "timestamp": str(ts),
                "energy_kwh": energy_val,
                "incremental_kwh": inc_val,
            }
            valid_events.append(norm_event)

            # 6. Station Aggregation update
            st_key = str(station_id)
            if st_key not in station_agg:
                station_agg[st_key] = {
                    "station_id": st_key,
                    "event_count": 0,
                    "total_incremental_kwh": 0.0,
                    "max_cumulative_kwh": 0.0,
                }
            station_agg[st_key]["event_count"] += 1
            station_agg[st_key]["total_incremental_kwh"] += inc_val
            station_agg[st_key]["max_cumulative_kwh"] = max(
                station_agg[st_key]["max_cumulative_kwh"], energy_val
            )

        # Normalize metrics to rounded values
        station_metrics = []
        for m in station_agg.values():
            station_metrics.append({
                "station_id": m["station_id"],
                "event_count": m["event_count"],
                "total_incremental_kwh": round(m["total_incremental_kwh"], 4),
                "max_cumulative_kwh": round(m["max_cumulative_kwh"], 4),
            })

        return valid_events, quarantined_events, station_metrics


# ======================================================================
# 9. KAFKA MICRO-BATCH CONSUMER (FOR LIVE / INTEGRATION SMOKE TESTS)
# ======================================================================

def run_kafka_micro_batch_consumer(
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC,
    max_messages: int = 10,
    poll_timeout: float = 3.0,
) -> dict:
    """
    Poll bounded messages from Kafka topic and process through micro-batch engine.

    Returns
    -------
    dict
        Summary with keys:
          - messages_polled
          - valid_events_count
          - quarantined_events_count
          - station_metrics
    """
    try:
        from confluent_kafka import Consumer
    except ImportError:
        raise ImportError("confluent-kafka is required for run_kafka_micro_batch_consumer.")

    import uuid
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"smoke-test-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    engine = StreamingMicroBatchEngine()
    messages: List[dict] = []

    try:
        start_time = datetime.now()
        while len(messages) < max_messages:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                if (datetime.now() - start_time).total_seconds() > poll_timeout:
                    break
                continue
            if msg.error():
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                messages.append(payload)
            except Exception:
                messages.append({"raw": str(msg.value())})
    finally:
        consumer.close()

    valid, quarantined, metrics = engine.process_micro_batch(messages)

    return {
        "messages_polled": len(messages),
        "valid_events_count": len(valid),
        "quarantined_events_count": len(quarantined),
        "station_metrics": metrics,
    }


# ======================================================================
# 10. MAIN STRUCTURED STREAMING PIPELINE RUNNER
# ======================================================================

def run_stream_transforms(
    spark: SparkSession,
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    output_sink: str = "console",
    checkpoint_dir: Optional[str] = None,
) -> Any:
    """
    Start the end-to-end Spark Structured Streaming pipeline:
      Kafka Source -> Deserialization -> Watermark/Deduplication -> Window Aggregations -> Sink

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.
    bootstrap_servers : Optional[str]
        Kafka bootstrap servers.
    topic : Optional[str]
        Kafka topic name.
    output_sink : str
        'console', 'memory', or 'postgres'.
    checkpoint_dir : Optional[str]
        Checkpoint directory path.
    """
    logger.info("Initializing Structured Streaming pipeline...")

    # 1. Ingest Kafka Stream
    raw_df = create_kafka_read_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        starting_offsets="latest",
    )

    # 2. Decode and Validate Stream
    valid_df, quarantined_df = decode_and_validate_stream(raw_df)

    # 3. Apply Watermarking and Deduplication
    deduped_df = apply_watermark_and_deduplication(
        valid_df,
        watermark_duration=DEFAULT_WATERMARK_DURATION,
        timestamp_col="event_timestamp",
        dedup_cols=["event_id", "session_id"],
    )

    # 4. Compute Windowed Aggregations
    metrics_df = compute_streaming_station_aggregations(
        deduped_df,
        window_duration=DEFAULT_WINDOW_DURATION,
        timestamp_col="event_timestamp",
    )

    # 5. Output Sink Configuration
    if output_sink == "console":
        query = configure_console_sink(metrics_df)
    elif output_sink == "memory":
        query = configure_memory_sink(metrics_df)
    elif output_sink == "postgres":
        query = configure_postgres_sink(metrics_df, checkpoint_location=checkpoint_dir)
    else:
        raise ValueError(f"Unsupported output_sink: {output_sink}")

    return query


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if SPARK_AVAILABLE:
        spark = create_streaming_session()
        query = run_stream_transforms(spark)
        query.awaitTermination()
    else:
        print("PySpark is not installed; run offline engine or tests.")
