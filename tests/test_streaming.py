"""
tests/test_streaming.py — Test suite for real-time Kafka streaming and Spark Structured Streaming.

Focus Areas:
  1. Event Generator & Data Contract Conformance
  2. Kafka Producer & Smoke Test Connectivity
  3. PySpark Schema Definitions & Validation
  4. Stream Decoding, Quarantine & Anomaly Handling
  5. Event-Time Watermarking & Stateful Deduplication
  6. Tumbling & Sliding Windowed Aggregations
  7. Multi-Sink Configuration & Graceful Shutdown
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

try:
    from pyspark.sql import functions as F
except ImportError:
    F = None

from processing.stream_transform import (
    CHARGING_EVENTS_COLUMNS,
    POSTGRES_EVENTS_TABLE,
    POSTGRES_METRICS_TABLE,
    POSTGRES_TABLE,
    SPARK_AVAILABLE,
    STATION_WINDOW_METRICS_COLUMNS,
    CompositeStreamingQuery,
    StreamingMicroBatchEngine,
    apply_watermark_and_deduplication,
    compute_streaming_station_aggregations,
    configure_console_sink,
    configure_memory_sink,
    configure_postgres_events_sink,
    configure_postgres_metrics_sink,
    configure_postgres_sink,
    create_kafka_read_stream,
    create_streaming_session,
    decode_and_validate_stream,
    get_charging_events_postgres_schema,
    get_station_aggregation_spark_schema,
    get_station_window_metrics_postgres_schema,
    get_streaming_event_spark_schema,
    parse_and_transform_event,
    stop_query_gracefully,
    write_postgres_micro_batch,
)
from streaming.event_generator import generate_event, stream_events
from streaming.kafka_smoke_test import check_kafka_connectivity
from streaming.producer import create_producer, publish_event


# ======================================================================
# 1. EVENT GENERATOR TESTS
# ======================================================================

def test_generate_event_has_required_fields():
    """Each event must contain all fields required by data_contracts.yaml."""
    event = generate_event()
    required_fields = {
        "event_id",
        "station_id",
        "session_id",
        "event_type",
        "timestamp",
        "energy_kwh",
        "incremental_kwh",
    }
    assert required_fields.issubset(event.keys()), f"Missing fields: {required_fields - event.keys()}"


def test_generate_event_energy_range():
    """energy_kwh should be between 0 and 75."""
    event = generate_event()
    assert 0.0 <= event["energy_kwh"] <= 75.0
    assert 0.0 <= event["incremental_kwh"] <= event["energy_kwh"]


def test_generate_event_with_custom_parameters():
    """Verify generate_event honors user-provided parameters."""
    event = generate_event(
        station_id="2-39-81-4550",
        event_type="session_start",
        cumulative_kwh=0.0,
        incremental_kwh=0.0,
    )
    assert event["station_id"] == "2-39-81-4550"
    assert event["event_type"] == "session_start"
    assert event["energy_kwh"] == 0.0
    assert event["incremental_kwh"] == 0.0


def test_stream_events_generator():
    """Verify stream_events yields continuous events."""
    gen = stream_events(interval_seconds=0.001)
    events = [next(gen) for _ in range(3)]
    assert len(events) == 3
    assert all("event_id" in e for e in events)


# ======================================================================
# 2. KAFKA PRODUCER & SMOKE TEST CONNECTIVITY
# ======================================================================

def test_kafka_smoke_test_unreachable_broker():
    """Smoke test helper cleanly returns False for an unreachable host."""
    assert check_kafka_connectivity("127.0.0.1:65530", timeout_seconds=0.2) is False


def test_producer_publish_event_calls_underlying_producer():
    """Verify publish_event serializes and delegates to producer.produce."""
    mock_producer = MagicMock()
    event = {
        "event_id": "evt-mock-1",
        "station_id": "2-39-81-4550",
        "session_id": "sess-mock-1",
        "event_type": "energy_update",
        "timestamp": "2026-10-24T12:00:00+00:00",
        "energy_kwh": 10.5,
        "incremental_kwh": 2.5,
    }

    publish_event(mock_producer, event, topic="custom-charging-topic")

    assert mock_producer.produce.called
    kwargs = mock_producer.produce.call_args[1]
    assert kwargs["topic"] == "custom-charging-topic"
    assert kwargs["key"] == b"2-39-81-4550"
    assert b"evt-mock-1" in kwargs["value"]
    assert mock_producer.poll.called


# ======================================================================
# 3. SCHEMA DEFINITIONS & PARSING
# ======================================================================

def test_streaming_event_spark_schema_conformance():
    """Verify PySpark streaming schema strictly matches contracts/data_contracts.yaml."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    schema = get_streaming_event_spark_schema()
    fields = [f.name for f in schema.fields]
    assert fields == [
        "event_id",
        "station_id",
        "session_id",
        "event_type",
        "timestamp",
        "energy_kwh",
        "incremental_kwh",
    ]


def test_station_aggregation_spark_schema_conformance():
    """Verify station aggregation schema matches expected metric output fields."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    schema = get_station_aggregation_spark_schema()
    fields = [f.name for f in schema.fields]
    assert "station_id" in fields
    assert "event_count" in fields
    assert "total_incremental_kwh" in fields
    assert "max_cumulative_kwh" in fields


def test_parse_and_transform_event_valid_and_invalid():
    """Verify JSON parsing and error handling for malformed input."""
    valid_payload = json.dumps({
        "event_id": "evt-val-1",
        "station_id": "2-39-81-4550",
        "session_id": "sess-1",
        "event_type": "energy_update",
        "timestamp": "2026-10-24T12:00:00+00:00",
        "energy_kwh": 15.0,
        "incremental_kwh": 3.0,
    })
    res = parse_and_transform_event(valid_payload)
    assert res["event_id"] == "evt-val-1"
    assert res["energy_kwh"] == 15.0

    # Missing station_id
    with pytest.raises(ValueError, match="missing_required_field:station_id"):
        parse_and_transform_event(json.dumps({
            "event_id": "evt-bad",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-24T12:00:00+00:00",
            "energy_kwh": 10.0,
        }))

    # Negative energy
    with pytest.raises(ValueError, match="negative_energy_kwh"):
        parse_and_transform_event(json.dumps({
            "event_id": "evt-bad",
            "station_id": "2-39-81-4550",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-24T12:00:00+00:00",
            "energy_kwh": -5.0,
        }))


# ======================================================================
# 4. MICRO-BATCH ENGINE DEDUPLICATION & AGGREGATION SEMANTICS
# ======================================================================

def test_micro_batch_engine_stateful_deduplication():
    """Verify duplicate event IDs are quarantined as duplicate_event_id."""
    engine = StreamingMicroBatchEngine()
    e1 = {
        "event_id": "evt-dup-1",
        "station_id": "2-39-81-4550",
        "session_id": "sess-1",
        "event_type": "session_start",
        "timestamp": "2026-10-24T12:00:00+00:00",
        "energy_kwh": 0.0,
        "incremental_kwh": 0.0,
    }
    e2 = {
        "event_id": "evt-dup-2",
        "station_id": "2-39-81-4550",
        "session_id": "sess-1",
        "event_type": "energy_update",
        "timestamp": "2026-10-24T12:05:00+00:00",
        "energy_kwh": 3.0,
        "incremental_kwh": 3.0,
    }

    # Micro-batch contains duplicates of e1 and e2
    valid, quarantined, metrics = engine.process_micro_batch([e1, e2, e1, e2])
    assert len(valid) == 2
    assert len(quarantined) == 2
    assert all(q["quarantine_reason"] == "duplicate_event_id" for q in quarantined)
    assert len(metrics) == 1
    assert metrics[0]["station_id"] == "2-39-81-4550"
    assert metrics[0]["total_incremental_kwh"] == 3.0
    assert metrics[0]["max_cumulative_kwh"] == 3.0


def test_micro_batch_engine_quarantine_unknown_station_and_anomalies():
    """Verify unknown stations, negative energy, and non-zero session_starts are caught."""
    engine = StreamingMicroBatchEngine()
    events = [
        # Unknown station ID outside verified fleet
        {
            "event_id": "evt-anom-1",
            "station_id": "station-unknown-999",
            "session_id": "sess-bad",
            "event_type": "energy_update",
            "timestamp": "2026-10-24T12:00:00+00:00",
            "energy_kwh": 10.0,
        },
        # Session start with non-zero energy
        {
            "event_id": "evt-anom-2",
            "station_id": "2-39-81-4550",
            "session_id": "sess-bad2",
            "event_type": "session_start",
            "timestamp": "2026-10-24T12:00:00+00:00",
            "energy_kwh": 5.0,
        },
        # Negative energy
        {
            "event_id": "evt-anom-3",
            "station_id": "2-39-81-4550",
            "session_id": "sess-bad3",
            "event_type": "energy_update",
            "timestamp": "2026-10-24T12:00:00+00:00",
            "energy_kwh": -2.0,
        },
    ]

    valid, quarantined, metrics = engine.process_micro_batch(events)
    assert len(valid) == 0
    assert len(quarantined) == 3
    reasons = [q["quarantine_reason"] for q in quarantined]
    assert "unknown_station_id" in reasons
    assert "non_zero_start_energy" in reasons
    assert "negative_energy_kwh" in reasons


# ======================================================================
# 5. PYSPARK STRUCTURED STREAMING PIPELINE TESTS
# ======================================================================

def test_pyspark_streaming_watermark_dedup_and_aggregation():
    """
    Verify complete Spark Structured Streaming DataFrame pipeline:
    decoding -> watermarking -> deduplication -> windowed aggregation.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    spark = create_streaming_session(app_name="Test-Streaming-Pipeline")
    try:
        sample_messages = [
            (
                "evt-1",
                json.dumps({
                    "event_id": "evt-00000000-0000-0000-0000-000000000001",
                    "station_id": "2-39-81-4550",
                    "session_id": "sess-1",
                    "event_type": "session_start",
                    "timestamp": "2026-10-24T12:00:00+00:00",
                    "energy_kwh": 0.0,
                    "incremental_kwh": 0.0,
                }),
                "charging-events",
                0,
                101,
                datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc),
            ),
            (
                "evt-2",
                json.dumps({
                    "event_id": "evt-00000000-0000-0000-0000-000000000002",
                    "station_id": "2-39-81-4550",
                    "session_id": "sess-1",
                    "event_type": "energy_update",
                    "timestamp": "2026-10-24T12:05:00+00:00",
                    "energy_kwh": 4.5,
                    "incremental_kwh": 4.5,
                }),
                "charging-events",
                0,
                102,
                datetime(2026, 10, 24, 12, 5, 0, tzinfo=timezone.utc),
            ),
            # Malformed payload to test quarantine path
            (
                "evt-bad",
                "malformed-json-payload{",
                "charging-events",
                0,
                103,
                datetime(2026, 10, 24, 12, 6, 0, tzinfo=timezone.utc),
            ),
        ]
        columns = ["key", "value", "topic", "partition", "offset", "timestamp"]
        raw_df = spark.createDataFrame(sample_messages, columns)

        # 1. Decode & Validate
        valid_df, quarantined_df = decode_and_validate_stream(raw_df)
        assert "event_id" in valid_df.columns
        assert "event_timestamp" in valid_df.columns
        assert "quarantine_reason" in quarantined_df.columns

        assert valid_df.count() == 2
        assert quarantined_df.count() == 1

        # 2. Watermark & Deduplication
        deduped_df = apply_watermark_and_deduplication(
            valid_df,
            watermark_duration="10 minutes",
            timestamp_col="event_timestamp",
            dedup_cols=["event_id", "session_id"],
        )
        assert deduped_df.count() == 2

        # 3. Windowed Aggregations
        metrics_df = compute_streaming_station_aggregations(
            deduped_df,
            window_duration="10 minutes",
            timestamp_col="event_timestamp",
        )
        cols = metrics_df.columns
        assert "station_id" in cols
        assert "event_count" in cols
        assert "total_incremental_kwh" in cols
        assert "max_cumulative_kwh" in cols

        rows = metrics_df.collect()
        assert len(rows) >= 1
        station_row = [r for r in rows if r["station_id"] == "2-39-81-4550"][0]
        assert station_row["event_count"] == 2
        assert station_row["total_incremental_kwh"] == 4.5
        assert station_row["max_cumulative_kwh"] == 4.5
    finally:
        spark.stop()


def test_streaming_drop_duplicates_within_watermark_on_real_streaming_df():
    """Verify dropDuplicatesWithinWatermark functions on an actual streaming DataFrame."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    spark = create_streaming_session(app_name="Test-Streaming-Dedup-Rate")
    try:
        # Use rate source to create a true Structured Streaming DataFrame
        rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
        assert rate_stream.isStreaming is True

        event_stream = (
            rate_stream
            .withColumn("event_id", F.concat(F.lit("evt-"), F.col("value")))
            .withColumn("session_id", F.lit("sess-rate-1"))
            .withColumn("station_id", F.lit("2-39-81-4550"))
            .withColumn("event_type", F.lit("energy_update"))
            .withColumn("event_timestamp", F.col("timestamp"))
            .withColumn("energy_kwh", F.lit(5.0))
            .withColumn("incremental_kwh", F.lit(0.5))
        )

        deduped = apply_watermark_and_deduplication(
            event_stream,
            watermark_duration="10 minutes",
            timestamp_col="event_timestamp",
            dedup_cols=["event_id", "session_id"],
        )
        assert deduped.isStreaming is True

        query = (
            deduped.writeStream
            .format("memory")
            .queryName("test_streaming_dedup_sink")
            .outputMode("append")
            .start()
        )
        assert query.isActive is True
        query.processAllAvailable()
        assert stop_query_gracefully(query, timeout_seconds=2.0) is True
    finally:
        spark.stop()


# ======================================================================
# 6. SINK CONFIGURATION & QUERY LIFECYCLE
# ======================================================================

def test_stop_query_gracefully_handles_none_and_active_queries():
    """Verify stop_query_gracefully cleanly terminates queries and accepts None."""
    assert stop_query_gracefully(None) is True

    mock_query = MagicMock()
    assert stop_query_gracefully(mock_query, timeout_seconds=1.0) is True
    mock_query.stop.assert_called_once()
    mock_query.awaitTermination.assert_called_once_with(timeout=1.0)


# ======================================================================
# 7. POSTGRESQL STREAMING SINK TESTS (charging_events & station_window_metrics)
# ======================================================================

def test_charging_events_postgres_schema_conformance():
    """Verify charging_events sink schema matches database/schema.sql."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    schema = get_charging_events_postgres_schema()
    fields = [f.name for f in schema.fields]
    assert fields == [
        "event_id",
        "station_id",
        "session_id",
        "event_type",
        "event_timestamp",
        "energy_kwh",
        "incremental_kwh",
    ]
    # In relational schema, timestamp and kafka_timestamp must NOT exist
    assert "timestamp" not in fields
    assert "kafka_timestamp" not in fields


def test_station_window_metrics_postgres_schema_conformance():
    """Verify station_window_metrics sink schema matches database/schema.sql."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    schema = get_station_window_metrics_postgres_schema()
    fields = [f.name for f in schema.fields]
    assert fields == [
        "station_id",
        "window_start",
        "window_end",
        "event_count",
        "total_incremental_kwh",
        "max_cumulative_kwh",
    ]


def test_charging_events_sink_column_filtering():
    """
    Verify individual events sink drops timestamp and kafka_timestamp,
    and retains strictly canonical columns before writing to PostgreSQL.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    spark = create_streaming_session(app_name="Test-Events-Sink-Columns")
    try:
        sample_messages = [
            (
                "evt-1",
                json.dumps({
                    "event_id": "evt-00000000-0000-0000-0000-000000000001",
                    "station_id": "2-39-81-4550",
                    "session_id": "sess-sink-1",
                    "event_type": "energy_update",
                    "timestamp": "2026-10-24T12:00:00+00:00",
                    "energy_kwh": 10.0,
                    "incremental_kwh": 2.5,
                }),
                "charging-events",
                0,
                101,
                datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc),
            )
        ]
        columns = ["key", "value", "topic", "partition", "offset", "timestamp"]
        raw_df = spark.createDataFrame(sample_messages, columns)

        valid_df, _ = decode_and_validate_stream(raw_df)
        deduped_df = apply_watermark_and_deduplication(valid_df)

        # deduped_df still contains streaming helper columns
        assert "timestamp" in deduped_df.columns
        assert "kafka_timestamp" in deduped_df.columns

        # Projection for charging_events PostgreSQL sink
        events_df = deduped_df.select(*CHARGING_EVENTS_COLUMNS)

        # Strictly canonical columns
        assert events_df.columns == [
            "event_id",
            "station_id",
            "session_id",
            "event_type",
            "event_timestamp",
            "energy_kwh",
            "incremental_kwh",
        ]
        assert "timestamp" not in events_df.columns
        assert "kafka_timestamp" not in events_df.columns

        row = events_df.first()
        assert row["event_id"] == "evt-00000000-0000-0000-0000-000000000001"
        assert row["station_id"] == "2-39-81-4550"
        assert row["energy_kwh"] == 10.0
        assert row["incremental_kwh"] == 2.5
    finally:
        spark.stop()


def test_station_window_metrics_sink_column_mapping():
    """
    Verify windowed aggregates sink maps metrics_df to ensure exact
    relational columns are selected for station_window_metrics.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    spark = create_streaming_session(app_name="Test-Metrics-Sink-Columns")
    try:
        sample_messages = [
            (
                "evt-1",
                json.dumps({
                    "event_id": "evt-00000000-0000-0000-0000-000000000001",
                    "station_id": "2-39-81-4550",
                    "session_id": "sess-metric-1",
                    "event_type": "energy_update",
                    "timestamp": "2026-10-24T12:00:00+00:00",
                    "energy_kwh": 5.0,
                    "incremental_kwh": 5.0,
                }),
                "charging-events",
                0,
                101,
                datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc),
            )
        ]
        columns = ["key", "value", "topic", "partition", "offset", "timestamp"]
        raw_df = spark.createDataFrame(sample_messages, columns)

        valid_df, _ = decode_and_validate_stream(raw_df)
        deduped_df = apply_watermark_and_deduplication(valid_df)
        metrics_df = compute_streaming_station_aggregations(deduped_df)

        mapped_metrics_df = metrics_df.select(*STATION_WINDOW_METRICS_COLUMNS)

        assert mapped_metrics_df.columns == [
            "station_id",
            "window_start",
            "window_end",
            "event_count",
            "total_incremental_kwh",
            "max_cumulative_kwh",
        ]

        row = mapped_metrics_df.first()
        assert row["station_id"] == "2-39-81-4550"
        assert row["event_count"] == 1
        assert row["total_incremental_kwh"] == 5.0
        assert row["max_cumulative_kwh"] == 5.0
    finally:
        spark.stop()


def test_write_postgres_micro_batch_delegates_to_jdbc_writer():
    """Verify write_postgres_micro_batch correctly formats JDBC parameters and options."""
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False
    mock_df.columns = [
        "event_id",
        "station_id",
        "session_id",
        "event_type",
        "timestamp",
        "event_timestamp",
        "energy_kwh",
        "incremental_kwh",
        "kafka_timestamp",
    ]

    mock_selected = MagicMock()
    mock_df.select.return_value = mock_selected

    mock_write = MagicMock()
    mock_format = MagicMock()
    mock_opt1 = MagicMock()
    mock_opt2 = MagicMock()
    mock_opt3 = MagicMock()
    mock_opt4 = MagicMock()
    mock_opt5 = MagicMock()
    mock_mode = MagicMock()

    mock_selected.write = mock_write
    mock_write.format.return_value = mock_format
    mock_format.option.return_value = mock_opt1
    mock_opt1.option.return_value = mock_opt2
    mock_opt2.option.return_value = mock_opt3
    mock_opt3.option.return_value = mock_opt4
    mock_opt4.option.return_value = mock_opt5
    mock_opt5.mode.return_value = mock_mode

    write_postgres_micro_batch(
        mock_df,
        batch_id=1,
        table_name="charging_events",
        db_config={
            "host": "localhost",
            "port": "5432",
            "database": "ev_charging_db",
            "user": "postgres",
            "password": "secret_password",
        },
    )

    # Verify select was called with strictly canonical columns
    mock_df.select.assert_called_once_with(*CHARGING_EVENTS_COLUMNS)

    # Verify JDBC write flow
    mock_write.format.assert_called_once_with("jdbc")
    mock_mode.save.assert_called_once()


def test_composite_streaming_query_lifecycle():
    """Verify CompositeStreamingQuery delegates isActive, stop, awaitTermination, and indexing."""
    q1 = MagicMock()
    q2 = MagicMock()
    q1.isActive = True
    q2.isActive = False

    comp = CompositeStreamingQuery([q1, q2])

    assert comp.isActive is True
    assert len(comp) == 2
    assert comp[0] == q1
    assert comp[1] == q2

    # Test iteration
    items = list(comp)
    assert items == [q1, q2]

    # Test stop & graceful shutdown
    assert stop_query_gracefully(comp, timeout_seconds=1.5) is True
    q1.stop.assert_called_once()
    q2.stop.assert_called_once()
    q1.awaitTermination.assert_called_once_with(timeout=1.5)
    q2.awaitTermination.assert_called_once_with(timeout=1.5)
