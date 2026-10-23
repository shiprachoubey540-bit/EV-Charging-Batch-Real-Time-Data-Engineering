"""
tests/test_processing.py — Offline unit tests for processing.batch_transform.

All tests run completely offline without requiring a live Spark cluster, Kafka,
or external databases.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

from processing.batch_transform import (
    SPARK_AVAILABLE,
    calculate_avg_power_kw,
    calculate_duration_minutes,
    create_spark_session,
    get_daily_aggregation_spark_schema,
    get_historical_session_spark_schema,
    get_station_aggregation_spark_schema,
    get_user_inputs_spark_schema,
    read_dataframe_safe,
    rfc2822_to_iso,
    run_batch_transforms,
    transform_records_tabular,
    validate_and_classify_record,
)
from processing.verify_batch import (
    assert_staging_readiness,
    verify_historical_batch,
)


# ======================================================================
# FIXTURES
# ======================================================================

@pytest.fixture
def sample_valid_record():
    return {
        "_id": "rec_001",
        "sessionID": "2_39_81_4550_2021-09-14 01:52:31",
        "stationID": "2-39-81-4550",
        "spaceID": "11900388",
        "siteID": "0002",
        "clusterID": "0039",
        "connectionTime": "Tue, 14 Sep 2021 01:52:37 GMT",
        "disconnectTime": "Tue, 14 Sep 2021 03:05:10 GMT",
        "doneChargingTime": "Tue, 14 Sep 2021 02:30:00 GMT",
        "kWhDelivered": 15.5,
        "userID": "000019055",
        "timezone": "America/Los_Angeles",
        "userInputs": [
            {
                "userID": 19055,
                "milesRequested": 50.0,
                "WhPerMile": 300.0,
                "minutesAvailable": 120.0,
                "modifiedAt": "Tue, 14 Sep 2021 01:52:37 GMT",
                "paymentRequired": True,
                "requestedDeparture": "Tue, 14 Sep 2021 04:00:00 GMT",
                "kWhRequested": 15.0,
            }
        ],
    }


# ======================================================================
# HELPER & NORMALIZATION TESTS
# ======================================================================

def test_rfc2822_to_iso_valid():
    """Valid RFC 2822 strings must convert to ISO 8601 UTC."""
    raw = "Tue, 14 Sep 2021 01:52:37 GMT"
    iso = rfc2822_to_iso(raw)
    assert iso == "2021-09-14T01:52:37+00:00"


def test_rfc2822_to_iso_invalid_or_none():
    """Invalid or None inputs must return None safely."""
    assert rfc2822_to_iso(None) is None
    assert rfc2822_to_iso("") is None
    assert rfc2822_to_iso("invalid date format") is None


def test_calculate_duration_minutes():
    """Verify duration computation in minutes."""
    start = "2021-09-14T01:00:00+00:00"
    end = "2021-09-14T02:30:00+00:00"
    dur = calculate_duration_minutes(start, end)
    assert dur == 90.0

    # Null handling
    assert calculate_duration_minutes(None, end) is None
    assert calculate_duration_minutes(start, None) is None


# ======================================================================
# VALIDATION & QUARANTINE CLASSIFICATION TESTS
# ======================================================================

def test_validate_valid_record(sample_valid_record):
    """Valid record must be classified as clean with no quarantine reasons."""
    is_valid, flags = validate_and_classify_record(sample_valid_record)
    assert is_valid is True
    assert flags == []


def test_quarantine_missing_identifiers(sample_valid_record):
    """Records missing required identifiers must be quarantined."""
    for field in ["_id", "sessionID", "siteID", "stationID"]:
        bad_rec = dict(sample_valid_record)
        bad_rec[field] = None
        is_valid, reasons = validate_and_classify_record(bad_rec)
        assert is_valid is False
        assert any(f"missing_required_identifier:{field}" in r for r in reasons)


def test_quarantine_invalid_timestamps(sample_valid_record):
    """Records with missing or unparsable timestamps must be quarantined."""
    # Missing connection time
    rec_no_conn = dict(sample_valid_record)
    rec_no_conn["connectionTime"] = None
    is_valid, reasons = validate_and_classify_record(rec_no_conn)
    assert is_valid is False
    assert any("missing_required_timestamp:connectionTime" in r for r in reasons)

    # Unparsable disconnect time
    rec_bad_disc = dict(sample_valid_record)
    rec_bad_disc["disconnectTime"] = "not-a-timestamp"
    is_valid, reasons = validate_and_classify_record(rec_bad_disc)
    assert is_valid is False
    assert any("unparsable_timestamp:disconnectTime" in r for r in reasons)


def test_quarantine_chronological_inversion(sample_valid_record):
    """Connection time after disconnect time must be quarantined."""
    bad_rec = dict(sample_valid_record)
    # Plugged in at 03:00, disconnected at 01:00
    bad_rec["connectionTime"] = "Tue, 14 Sep 2021 03:00:00 GMT"
    bad_rec["disconnectTime"] = "Tue, 14 Sep 2021 01:00:00 GMT"

    is_valid, reasons = validate_and_classify_record(bad_rec)
    assert is_valid is False
    assert any("chronological_inversion" in r for r in reasons)


def test_quarantine_negative_energy(sample_valid_record):
    """Negative kWhDelivered must be quarantined."""
    bad_rec = dict(sample_valid_record)
    bad_rec["kWhDelivered"] = -5.0

    is_valid, reasons = validate_and_classify_record(bad_rec)
    assert is_valid is False
    assert any("invalid_energy:kWhDelivered_negative" in r for r in reasons)


def test_done_after_disconnect_advisory_flag(sample_valid_record):
    """doneChargingTime > disconnectTime is an advisory flag, not quarantined."""
    rec = dict(sample_valid_record)
    # Disconnect at 03:00, done at 03:30
    rec["disconnectTime"] = "Tue, 14 Sep 2021 03:00:00 GMT"
    rec["doneChargingTime"] = "Tue, 14 Sep 2021 03:30:00 GMT"

    is_valid, flags = validate_and_classify_record(rec)
    assert is_valid is True
    assert "done_after_disconnect" in flags


# ======================================================================
# TRANSFORMATION & DEDUPLICATION TESTS
# ======================================================================

def test_transform_empty_inputs():
    """Empty record list must return empty DataFrames with correct columns without errors."""
    sess_df, ui_df, quarantine, metrics = transform_records_tabular([])
    assert isinstance(sess_df, pd.DataFrame)
    assert isinstance(ui_df, pd.DataFrame)
    assert len(sess_df) == 0
    assert len(ui_df) == 0
    assert len(quarantine) == 0
    assert metrics["input_records"] == 0
    assert metrics["clean_records"] == 0
    assert metrics["quarantined_records"] == 0
    assert "session_id" in sess_df.columns
    assert "session_id" in ui_df.columns


def test_transform_clean_session_and_user_inputs(sample_valid_record):
    """Valid record must produce normalized session and user input rows."""
    sess_df, ui_df, quarantine, metrics = transform_records_tabular([sample_valid_record])
    assert len(sess_df) == 1
    assert len(ui_df) == 1
    assert len(quarantine) == 0

    row = sess_df.iloc[0]
    assert row["id"] == "rec_001"
    assert row["session_id"] == "2_39_81_4550_2021-09-14 01:52:31"
    assert row["kwh_delivered"] == 15.5
    assert row["connection_time"] == "2021-09-14T01:52:37+00:00"
    assert row["disconnect_time"] == "2021-09-14T03:05:10+00:00"
    assert row["session_duration_min"] is not None
    assert row["session_duration_min"] > 0
    assert row["charging_duration_min"] is not None
    assert row["idle_duration_min"] is not None

    ui_row = ui_df.iloc[0]
    assert ui_row["session_id"] == row["session_id"]
    assert ui_row["miles_requested"] == 50.0
    assert bool(ui_row["payment_required"]) is True


def test_transform_done_after_disconnect_sets_idle_null(sample_valid_record):
    """When doneChargingTime > disconnectTime, idle and charging duration must be null."""
    rec = dict(sample_valid_record)
    rec["disconnectTime"] = "Tue, 14 Sep 2021 03:00:00 GMT"
    rec["doneChargingTime"] = "Tue, 14 Sep 2021 03:30:00 GMT"

    sess_df, _, _, _ = transform_records_tabular([rec])
    assert len(sess_df) == 1
    row = sess_df.iloc[0]
    assert row["session_duration_min"] is not None
    assert pd.isna(row["idle_duration_min"])
    assert pd.isna(row["charging_duration_min"])
    assert "done_after_disconnect" in row["validation_flags"]


def test_deterministic_deduplication(sample_valid_record):
    """Duplicate sessionID or _id must be routed to quarantine."""
    rec1 = dict(sample_valid_record)
    rec2 = dict(sample_valid_record)  # Exact copy
    rec2["kWhDelivered"] = 20.0

    sess_df, _, quarantine, metrics = transform_records_tabular([rec1, rec2])

    # Exactly 1 kept in clean output, 1 quarantined
    assert len(sess_df) == 1
    assert len(quarantine) == 1
    assert metrics["duplicate_records"] == 1
    assert metrics["clean_records"] == 1
    assert metrics["quarantined_records"] == 1

    q_item = quarantine[0]
    assert "duplicate_session_id" in q_item["quarantine_reasons"]
    assert "duplicate_record_id" in q_item["quarantine_reasons"]


# ======================================================================
# END-TO-END PIPELINE & REPORT TESTS
# ======================================================================

def test_run_batch_transforms_end_to_end(tmp_path, sample_valid_record):
    """run_batch_transforms must read candidate JSON, write Parquet, and write audit report."""
    candidate_json = tmp_path / "candidate.json"
    out_dir = tmp_path / "processed"

    # 1 valid record, 1 invalid record missing stationID
    invalid_rec = dict(sample_valid_record)
    invalid_rec["_id"] = "rec_invalid"
    invalid_rec["sessionID"] = "session_invalid"
    invalid_rec["stationID"] = None

    payload = {
        "_meta": {
            "site": "caltech",
            "start": "Tue, 01 Jan 2019 16:18:00 GMT",
            "end": "Tue, 31 Dec 2024 16:19:00 GMT",
            "total_extracted": 2,
            "api_reported_total": 2,
        },
        "_items": [sample_valid_record, invalid_rec],
    }
    candidate_json.write_text(json.dumps(payload), encoding="utf-8")

    report = run_batch_transforms(
        input_path=candidate_json,
        output_dir=out_dir,
        engine="pandas",
    )

    # Check files created
    assert (out_dir / "sessions.parquet").exists()
    assert (out_dir / "user_inputs.parquet").exists()
    assert (out_dir / "quarantine.json").exists()
    assert (out_dir / "quarantined_records.parquet").exists()
    assert (out_dir / "etl_audit_report.json").exists()

    # Check record accounting in report
    acc = report["record_accounting"]
    assert acc["total_input_records"] == 2
    assert acc["clean_session_records"] == 1
    assert acc["quarantined_records"] == 1
    assert acc["reconciliation_balanced"] is True

    # Validate generated Parquet can be read back
    read_sessions = read_dataframe_safe(out_dir / "sessions.parquet")
    assert len(read_sessions) == 1
    assert read_sessions.iloc[0]["id"] == "rec_001"

    read_quarantine = json.loads((out_dir / "quarantine.json").read_text(encoding="utf-8"))
    assert len(read_quarantine) == 1
    assert any("missing_required_identifier:stationID" in r for r in read_quarantine[0]["quarantine_reasons"])


def test_verify_historical_batch_end_to_end(tmp_path, sample_valid_record):
    """verify_historical_batch must validate processed Parquet outputs and generate analytics summaries."""
    from processing.verify_batch import verify_historical_batch

    candidate_json = tmp_path / "candidate.json"
    proc_dir = tmp_path / "processed"
    val_dir = tmp_path / "validation"

    payload = {
        "_meta": {
            "site": "caltech",
            "start": "Tue, 01 Jan 2019 16:18:00 GMT",
            "end": "Tue, 31 Dec 2024 16:19:00 GMT",
            "total_extracted": 1,
            "api_reported_total": 1,
        },
        "_items": [sample_valid_record],
    }
    candidate_json.write_text(json.dumps(payload), encoding="utf-8")

    run_batch_transforms(
        input_path=candidate_json,
        output_dir=proc_dir,
        engine="pandas",
    )

    verif_report = verify_historical_batch(
        historical_dir=proc_dir,
        raw_candidate_path=candidate_json,
        validation_dir=val_dir,
    )

    assert verif_report["verification_status"] == "PASSED"
    assert verif_report["all_checks_passed"] is True
    assert verif_report["checks"]["reconciliation"]["passed"] is True
    assert verif_report["checks"]["identifier_uniqueness"]["passed"] is True
    assert verif_report["checks"]["referential_integrity"]["passed"] is True
    assert verif_report["checks"]["timestamp_utc_compliance"]["passed"] is True
    assert verif_report["checks"]["non_negative_values"]["passed"] is True

    # Check generated validation files
    assert (val_dir / "verification_report.json").exists()
    assert (val_dir / "monthly_sessions_summary.csv").exists()
    assert (val_dir / "daily_sessions_summary.csv").exists()
    assert (val_dir / "station_summary.csv").exists()
    assert (val_dir / "hourly_peak_summary.csv").exists()
    assert (val_dir / "energy_and_duration_metrics.json").exists()


# ======================================================================
# PYSPARK BATCH TRANSFORM & STAGING ASSERTION UNIT TESTS
# ======================================================================

def test_calculate_avg_power_kw():
    """Verify average charging power calculation in kW."""
    # 10 kWh in 60 minutes = 10.0 kW
    assert calculate_avg_power_kw(10.0, 60.0) == 10.0
    # 25 kWh in 120 minutes = 12.5 kW
    assert calculate_avg_power_kw(25.0, 120.0) == 12.5
    # Zero or negative duration returns None safely
    assert calculate_avg_power_kw(10.0, 0.0) is None
    assert calculate_avg_power_kw(10.0, -15.0) is None
    # None inputs
    assert calculate_avg_power_kw(None, 60.0) is None
    assert calculate_avg_power_kw(10.0, None) is None


def test_spark_session_factory_graceful_or_configured():
    """Verify create_spark_session handles config overrides and returns None gracefully if PySpark is absent."""
    overrides = {"spark.sql.shuffle.partitions": "2"}
    spark = create_spark_session(app_name="test-batch-app", config_overrides=overrides)
    if not SPARK_AVAILABLE:
        assert spark is None
    else:
        assert spark is not None
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
        assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
        spark.stop()


def test_spark_schemas_conformance():
    """Verify PySpark StructType schemas conform strictly to contracts/data_contracts.yaml."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the test environment")

    sess_schema = get_historical_session_spark_schema()
    fields = [f.name for f in sess_schema.fields]
    assert "id" in fields
    assert "session_id" in fields
    assert "station_id" in fields
    assert "site_id" in fields
    assert "connection_time" in fields
    assert "disconnect_time" in fields
    assert "kwh_delivered" in fields
    assert "session_duration_min" in fields
    assert "charging_duration_min" in fields
    assert "idle_duration_min" in fields
    assert "avg_charging_power_kw" in fields

    ui_schema = get_user_inputs_spark_schema()
    ui_fields = [f.name for f in ui_schema.fields]
    assert "session_id" in ui_fields
    assert "user_id_numeric" in ui_fields
    assert "payment_required" in ui_fields

    station_schema = get_station_aggregation_spark_schema()
    st_fields = [f.name for f in station_schema.fields]
    assert "station_id" in st_fields
    assert "total_sessions" in st_fields
    assert "total_energy_kwh" in st_fields
    assert "avg_charging_power_kw" in st_fields

    daily_schema = get_daily_aggregation_spark_schema()
    d_fields = [f.name for f in daily_schema.fields]
    assert "session_date" in d_fields
    assert "total_sessions" in d_fields


def test_assert_staging_readiness_valid(sample_valid_record):
    """assert_staging_readiness must succeed on clean dataset adhering to contracts."""
    sess_df, ui_df, _, metrics = transform_records_tabular([sample_valid_record])
    audit_rep = {
        "record_accounting": {
            "clean_session_records": len(sess_df),
            "user_input_records": len(ui_df),
        }
    }
    result = assert_staging_readiness(sess_df, ui_df, audit_rep)
    assert result["staging_readiness"] == "PASSED"
    assert result["clean_sessions"] == 1


def test_assert_staging_readiness_detects_violations(sample_valid_record):
    """assert_staging_readiness must raise AssertionError on contract or quality violations."""
    sess_df, ui_df, _, _ = transform_records_tabular([sample_valid_record])

    # 1. Null primary key constraint violation
    bad_sess_df = sess_df.copy()
    bad_sess_df.loc[0, "session_id"] = None
    audit_rep = {
        "record_accounting": {
            "clean_session_records": len(bad_sess_df),
            "user_input_records": len(ui_df),
        }
    }
    with pytest.raises(AssertionError, match="Null constraint violated"):
        assert_staging_readiness(bad_sess_df, ui_df, audit_rep)

    # 2. Negative energy violation
    bad_energy_df = sess_df.copy()
    bad_energy_df.loc[0, "kwh_delivered"] = -10.0
    with pytest.raises(AssertionError, match="Non-negative constraint violated"):
        assert_staging_readiness(bad_energy_df, ui_df, audit_rep)

    # 3. Orphaned user_input foreign key violation
    bad_ui_df = ui_df.copy()
    bad_ui_df.loc[0, "session_id"] = "non_existent_session_id_999"
    with pytest.raises(AssertionError, match="Referential integrity failure"):
        assert_staging_readiness(sess_df, bad_ui_df, audit_rep)


def test_run_batch_transforms_on_sample_sessions_dataset(tmp_path):
    """run_batch_transforms must successfully process data/sample/sample_sessions.json."""
    sample_file = PROJECT_ROOT / "data" / "sample" / "sample_sessions.json"
    if not sample_file.exists():
        pytest.skip("data/sample/sample_sessions.json does not exist")

    out_dir = tmp_path / "sample_batch_out"
    report = run_batch_transforms(
        input_path=sample_file,
        output_dir=out_dir,
        engine="pandas",
    )

    assert report["pipeline_name"] == "historical_batch_etl"
    assert report["record_accounting"]["total_input_records"] >= 4
    assert report["record_accounting"]["clean_session_records"] >= 1
    assert report["record_accounting"]["reconciliation_balanced"] is True

    # Check generated files
    assert (out_dir / "sessions.parquet").exists()
    assert (out_dir / "user_inputs.parquet").exists()
    assert (out_dir / "quarantine.json").exists()
    assert (out_dir / "station_summary.csv").exists()
    assert (out_dir / "daily_summary.csv").exists()
    assert (out_dir / "hourly_peak_summary.csv").exists()
    assert (out_dir / "etl_audit_report.json").exists()

    clean_sessions = read_dataframe_safe(out_dir / "sessions.parquet")
    assert len(clean_sessions) == report["record_accounting"]["clean_session_records"]
    assert "avg_charging_power_kw" in clean_sessions.columns
# ======================================================================
# STREAM TRANSFORM COMPATIBILITY TESTS (FEATURE/STREAMING)
# ======================================================================

try:
    from processing.stream_transform import (  # noqa: F401
        StreamingMicroBatchEngine,
        parse_and_transform_event,
    )
    STREAM_TRANSFORM_AVAILABLE = True
except (ImportError, AttributeError):
    STREAM_TRANSFORM_AVAILABLE = False


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_schemas_match_contract():
    """Verify PySpark streaming schemas match contracts/data_contracts.yaml."""
    from processing.stream_transform import (
        SPARK_AVAILABLE,
        get_station_aggregation_spark_schema,
        get_streaming_event_spark_schema,
    )

    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    event_schema = get_streaming_event_spark_schema()
    event_fields = [f.name for f in event_schema.fields]
    assert event_fields == [
        "event_id",
        "station_id",
        "session_id",
        "event_type",
        "timestamp",
        "energy_kwh",
        "incremental_kwh",
    ]

    agg_schema = get_station_aggregation_spark_schema()
    agg_fields = [f.name for f in agg_schema.fields]
    assert "station_id" in agg_fields
    assert "event_count" in agg_fields
    assert "total_incremental_kwh" in agg_fields
    assert "max_cumulative_kwh" in agg_fields


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_valid_event_parsing():
    """Verify parse_and_transform_event parses and type-normalizes valid JSON payloads."""
    from processing.stream_transform import parse_and_transform_event

    raw_event = {
        "event_id": "evt-00000000-0000-0000-0000-000000000001",
        "station_id": "2-39-81-4550",
        "session_id": "2-39-81-4550_20261019120000",
        "event_type": "energy_update",
        "timestamp": "2026-10-19T12:15:00+00:00",
        "energy_kwh": 8.75,
        "incremental_kwh": 2.50,
    }
    payload_json = json.dumps(raw_event)

    result = parse_and_transform_event(payload_json)
    assert result["event_id"] == "evt-00000000-0000-0000-0000-000000000001"
    assert result["station_id"] == "2-39-81-4550"
    assert result["energy_kwh"] == 8.75
    assert result["incremental_kwh"] == 2.50


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_quarantine_invalid_json_and_missing_fields():
    """Verify malformed JSON and missing required fields are separated into quarantine."""
    from processing.stream_transform import StreamingMicroBatchEngine

    engine = StreamingMicroBatchEngine()
    batch = [
        "not_valid_json_payload{",
        {
            "event_id": "evt-missing-station",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-19T12:00:00+00:00",
            "energy_kwh": 5.0,
        },
    ]

    valid, quarantined, metrics = engine.process_micro_batch(batch)
    assert len(valid) == 0
    assert len(quarantined) == 2
    assert "unparseable_json" in quarantined[0]["quarantine_reason"]
    assert "missing_required_field:station_id" in quarantined[1]["quarantine_reason"]


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_quarantine_unknown_station_id():
    """Verify unknown station IDs outside verified fleet are quarantined."""
    from processing.stream_transform import StreamingMicroBatchEngine

    engine = StreamingMicroBatchEngine()
    bad_station_event = {
        "event_id": "evt-00000000-0000-0000-0000-000000000999",
        "station_id": "station-unknown-999",
        "session_id": "sess-999",
        "event_type": "energy_update",
        "timestamp": "2026-10-19T12:00:00+00:00",
        "energy_kwh": 5.0,
    }

    valid, quarantined, metrics = engine.process_micro_batch([bad_station_event])
    assert len(valid) == 0
    assert len(quarantined) == 1
    assert "unknown_station_id" in quarantined[0]["quarantine_reason"]


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_quarantine_negative_energy_and_invalid_start():
    """Verify negative energy and non-zero start energy are quarantined."""
    from processing.stream_transform import StreamingMicroBatchEngine

    engine = StreamingMicroBatchEngine()
    batch = [
        {
            "event_id": "evt-neg-1",
            "station_id": "2-39-81-4550",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-19T12:00:00+00:00",
            "energy_kwh": -2.0,
        },
        {
            "event_id": "evt-bad-start",
            "station_id": "2-39-81-4550",
            "session_id": "sess-2",
            "event_type": "session_start",
            "timestamp": "2026-10-19T12:00:00+00:00",
            "energy_kwh": 12.0,
        },
    ]

    valid, quarantined, metrics = engine.process_micro_batch(batch)
    assert len(valid) == 0
    assert len(quarantined) == 2
    assert "negative_energy_kwh" in quarantined[0]["quarantine_reason"]
    assert "non_zero_start_energy" in quarantined[1]["quarantine_reason"]


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_deduplication_by_event_id():
    """Verify streaming deduplication drops duplicate event_id deliveries."""
    from processing.stream_transform import StreamingMicroBatchEngine

    engine = StreamingMicroBatchEngine()
    e1 = {
        "event_id": "evt-dedup-1",
        "station_id": "2-39-81-4550",
        "session_id": "sess-1",
        "event_type": "session_start",
        "timestamp": "2026-10-19T12:00:00+00:00",
        "energy_kwh": 0.0,
        "incremental_kwh": 0.0,
    }
    e2 = {
        "event_id": "evt-dedup-2",
        "station_id": "2-39-81-4550",
        "session_id": "sess-1",
        "event_type": "energy_update",
        "timestamp": "2026-10-19T12:15:00+00:00",
        "energy_kwh": 5.0,
        "incremental_kwh": 5.0,
    }

    # Micro-batch with re-delivered events
    valid, quarantined, metrics = engine.process_micro_batch([e1, e2, e1, e2])
    assert len(valid) == 2
    assert len(quarantined) == 2
    assert quarantined[0]["quarantine_reason"] == "duplicate_event_id"
    assert quarantined[1]["quarantine_reason"] == "duplicate_event_id"


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_energy_aggregation_semantics():
    """Verify cumulative meter readings are never summed as interval energy."""
    from processing.stream_transform import StreamingMicroBatchEngine

    engine = StreamingMicroBatchEngine()
    events = [
        {
            "event_id": "evt-agg-1",
            "station_id": "2-39-81-4550",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-19T12:10:00+00:00",
            "energy_kwh": 2.0,
            "incremental_kwh": 2.0,
        },
        {
            "event_id": "evt-agg-2",
            "station_id": "2-39-81-4550",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-19T12:20:00+00:00",
            "energy_kwh": 5.0,
            "incremental_kwh": 3.0,
        },
        {
            "event_id": "evt-agg-3",
            "station_id": "2-39-81-4550",
            "session_id": "sess-1",
            "event_type": "energy_update",
            "timestamp": "2026-10-19T12:30:00+00:00",
            "energy_kwh": 9.0,
            "incremental_kwh": 4.0,
        },
    ]

    valid, quarantined, metrics = engine.process_micro_batch(events)
    assert len(valid) == 3
    assert len(metrics) == 1

    st = metrics[0]
    assert st["station_id"] == "2-39-81-4550"
    assert st["event_count"] == 3
    assert st["total_incremental_kwh"] == 9.0
    assert st["max_cumulative_kwh"] == 9.0


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_stream_transform_graceful_shutdown():
    """Verify stop_query_gracefully handles None and active queries cleanly."""
    from unittest.mock import MagicMock
    from processing.stream_transform import stop_query_gracefully

    # None query
    assert stop_query_gracefully(None) is True

    # Active query mock
    mock_query = MagicMock()
    assert stop_query_gracefully(mock_query, timeout_seconds=2.0) is True
    mock_query.stop.assert_called_once()
    mock_query.awaitTermination.assert_called_once_with(timeout=2.0)


@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_pyspark_streaming_dataframe_pipeline():
    """Verify PySpark streaming DataFrame pipeline construction and schema validation."""
    from datetime import datetime, timezone
    from processing.stream_transform import (
        SPARK_AVAILABLE,
        apply_watermark_and_deduplication,
        compute_streaming_station_aggregations,
        create_streaming_session,
        decode_and_validate_stream,
    )

    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed in the current environment")

    spark = create_streaming_session(app_name="Test-Pipeline")
    try:
        sample_records = [
            (
                "evt-1",
                json.dumps({
                    "event_id": "evt-00000000-0000-0000-0000-000000000001",
                    "station_id": "2-39-81-4550",
                    "session_id": "2-39-81-4550_20261019120000",
                    "event_type": "energy_update",
                    "timestamp": "2026-10-19T12:00:00+00:00",
                    "energy_kwh": 5.0,
                    "incremental_kwh": 2.5,
                }),
                "charging-events",
                0,
                100,
                datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc),
            )
        ]
        columns = ["key", "value", "topic", "partition", "offset", "timestamp"]
        raw_df = spark.createDataFrame(sample_records, columns)

        valid_df, quarantined_df = decode_and_validate_stream(raw_df)
        assert "event_id" in valid_df.columns
        assert "quarantine_reason" in quarantined_df.columns

        deduped_df = apply_watermark_and_deduplication(valid_df, watermark_duration="10 minutes")
        metrics_df = compute_streaming_station_aggregations(deduped_df)

        metrics_cols = metrics_df.columns
        assert "station_id" in metrics_cols
        assert "total_incremental_kwh" in metrics_cols
        assert "max_cumulative_kwh" in metrics_cols
    finally:
        spark.stop()


@pytest.mark.live_kafka
@pytest.mark.skipif(
    not STREAM_TRANSFORM_AVAILABLE,
    reason="Stream transform is implemented on the streaming feature branch",
)
def test_live_kafka_streaming_micro_batch_consumer():
    """
    Live integration test for consuming and aggregating events from Kafka.
    Automatically skipped if the local Kafka broker is unreachable.
    """
    from streaming.kafka_smoke_test import check_kafka_connectivity
    from streaming.producer import create_producer, publish_event
    from streaming.event_generator import generate_event
    from processing.stream_transform import run_kafka_micro_batch_consumer

    if not check_kafka_connectivity("localhost:9092", timeout_seconds=1.0):
        pytest.skip("Local Kafka broker not reachable at localhost:9092")

    # Publish 2 bounded test events to charging-events
    producer = create_producer({"bootstrap.servers": "localhost:9092"})
    try:
        e1 = generate_event(station_id="2-39-81-4550", event_type="session_start", cumulative_kwh=0.0)
        e2 = generate_event(station_id="2-39-81-4550", event_type="energy_update", cumulative_kwh=1.5, incremental_kwh=1.5)
        publish_event(producer, e1, topic="charging-events")
        publish_event(producer, e2, topic="charging-events")
        producer.flush(timeout=5.0)
    finally:
        pass

    # Consume using the micro-batch consumer
    result = run_kafka_micro_batch_consumer(
        bootstrap_servers="localhost:9092",
        topic="charging-events",
        max_messages=2,
        poll_timeout=3.0,
    )
    assert result["messages_polled"] >= 1
    assert result["valid_events_count"] >= 1
    assert len(result["station_metrics"]) >= 1
    for m in result["station_metrics"]:
        assert "station_id" in m
        assert "total_incremental_kwh" in m
        assert "max_cumulative_kwh" in m


