"""Tests for the ingestion module.

Uses the sample fixture at ``data/sample/sample_sessions.json`` which
contains 6 records covering normal, null, and anomaly cases:
    - Record 0: normal record with userInputs
    - Record 1: normal record with userInputs
    - Record 2: doneChargingTime = null (null done charging)
    - Record 3: userID = null, userInputs = null (anonymous session)
    - Record 4: doneChargingTime > disconnectTime (anomaly: ~58s telemetry lag)
    - Record 5: normal record, same user as record 0
"""

import csv
import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from ingestion.extract import extract_sessions, _repair_truncated_json, _check_date_range
from ingestion.validate import validate_sessions
from ingestion.transform import transform_sessions
from ingestion.load import load_sessions
from ingestion.run_pipeline import run_pipeline

SAMPLE_PATH = str(
    Path(__file__).resolve().parent.parent / "data" / "sample" / "sample_sessions.json"
)


# ======================================================================
# EXTRACT TESTS
# ======================================================================


class TestExtract:
    """Tests for ingestion.extract."""

    def test_extract_loads_json(self):
        """extract_sessions should return a list of dicts + metadata."""
        sessions, meta = extract_sessions(SAMPLE_PATH)
        assert isinstance(sessions, list)
        assert len(sessions) == 6
        assert isinstance(meta, dict)
        assert all(isinstance(s, dict) for s in sessions)

    def test_extract_returns_metadata_and_status(self):
        """_meta should contain site, start, end keys and _extraction_status."""
        _sessions, meta = extract_sessions(SAMPLE_PATH)
        assert meta.get("site") == "caltech"
        assert "start" in meta
        assert "end" in meta
        assert "_extraction_status" in meta
        status = meta["_extraction_status"]
        assert status["strict_parse_success"] is True
        assert status["recovery_mode_used"] is False

    def test_extract_session_has_expected_fields(self):
        """Each session record should have the 13 ACN-Data fields."""
        sessions, _meta = extract_sessions(SAMPLE_PATH)
        expected_keys = {
            "_id", "clusterID", "connectionTime", "disconnectTime",
            "doneChargingTime", "kWhDelivered", "sessionID", "siteID",
            "spaceID", "stationID", "timezone", "userID", "userInputs",
        }
        for s in sessions:
            assert expected_keys.issubset(s.keys()), f"Missing keys in {s.get('_id')}"

    def test_extract_handles_truncated_json(self):
        """Trailing comma + missing ]} should be repaired."""
        truncated = '{"_meta": {}, "_items": [{"a": 1}, {"a": 2},'
        repaired, appended = _repair_truncated_json(truncated)
        assert appended > 0
        data = json.loads(repaired)
        assert data["_items"] == [{"a": 1}, {"a": 2}]

    def test_extract_valid_json_passes_through(self):
        """Already-valid JSON should return with 0 appended bytes."""
        valid = '{"_meta": {}, "_items": [{"a": 1}]}'
        result, appended = _repair_truncated_json(valid)
        assert appended == 0
        assert json.loads(result) == json.loads(valid)

    def test_extract_file_not_found(self):
        """Non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            extract_sessions("/nonexistent/path.json")

    def test_extract_strict_mode_fails_on_truncated_json(self, tmp_path):
        """With allow_recovery=False, truncated JSON must raise JSONDecodeError."""
        truncated_file = tmp_path / "truncated.json"
        truncated_file.write_text('{"_meta": {}, "_items": [{"a": 1},', encoding="utf-8")
        with pytest.raises(json.JSONDecodeError):
            extract_sessions(str(truncated_file), allow_recovery=False)

    def test_extract_recovery_mode_marks_incomplete(self, tmp_path):
        """With allow_recovery=True, truncated JSON parses but marks incomplete."""
        truncated_file = tmp_path / "truncated.json"
        truncated_file.write_text('{"_meta": {}, "_items": [{"a": 1},', encoding="utf-8")
        sessions, meta = extract_sessions(str(truncated_file), allow_recovery=True)
        assert len(sessions) == 1
        status = meta["_extraction_status"]
        assert status["is_complete"] is False
        assert status["status"] == "INCOMPLETE_RECOVERED"
        assert status["recovery_mode_used"] is True
        assert status["strict_parse_success"] is False
        assert status["parse_error"] is not None

    def test_extract_detects_incomplete_date_range(self):
        """Compare requested range (6 years) vs records (3 months)."""
        sessions = [
            {"connectionTime": "Tue, 01 Jan 2019 17:41:45 GMT"},
            {"connectionTime": "Fri, 29 Mar 2019 19:26:09 GMT"},
        ]
        meta = {
            "start": "Tue, 01 Jan 2019 16:18:00 GMT",
            "end": "Tue, 31 Dec 2024 16:19:00 GMT",
        }
        res = _check_date_range(sessions, meta)
        assert res["date_range_complete"] is False
        assert "2019-01-01" in res["actual_start"]
        assert "2019-03-29" in res["actual_end"]


# ======================================================================
# VALIDATE TESTS
# ======================================================================


class TestValidate:
    """Tests for ingestion.validate."""

    @pytest.fixture()
    def raw_sessions(self):
        sessions, _meta = extract_sessions(SAMPLE_PATH)
        return sessions

    def test_validate_preserves_all_records(self, raw_sessions):
        """Validation must never drop records."""
        validated, _summary = validate_sessions(raw_sessions)
        assert len(validated) == len(raw_sessions)

    def test_validate_adds_flags_key(self, raw_sessions):
        """Every record should get a _validation_flags list."""
        validated, _summary = validate_sessions(raw_sessions)
        for rec in validated:
            assert "_validation_flags" in rec
            assert isinstance(rec["_validation_flags"], list)

    def test_validate_flags_done_after_disconnect(self, raw_sessions):
        """Record 4 has doneChargingTime > disconnectTime — should be flagged."""
        validated, summary = validate_sessions(raw_sessions)
        anomaly_rec = validated[4]
        assert "done_after_disconnect" in anomaly_rec["_validation_flags"]
        assert summary["flags"].get("done_after_disconnect", 0) >= 1

    def test_validate_flags_null_done_with_energy(self, raw_sessions):
        """Record 2 has doneChargingTime=null but kWhDelivered > 0."""
        validated, summary = validate_sessions(raw_sessions)
        null_done_rec = validated[2]
        assert "null_done_charging_time_with_energy" in null_done_rec["_validation_flags"]

    def test_validate_clean_records_have_no_flags(self, raw_sessions):
        """Normal records should have an empty flags list."""
        validated, _summary = validate_sessions(raw_sessions)
        assert validated[0]["_validation_flags"] == []
        assert validated[1]["_validation_flags"] == []

    def test_validate_summary_counts(self, raw_sessions):
        """Summary should report correct total, flagged, and duplicate counts."""
        _validated, summary = validate_sessions(raw_sessions)
        assert summary["total_records"] == 6
        assert summary["flagged_records"] >= 2
        assert "duplicate_records" in summary

    def test_validate_detects_duplicate_records(self, raw_sessions):
        """Duplicate _id and sessionID should be flagged and counted."""
        dup_sessions = [raw_sessions[0], dict(raw_sessions[0])]
        validated, summary = validate_sessions(dup_sessions)
        assert summary["duplicate_records"] == 1
        assert "duplicate_record_id" in validated[1]["_validation_flags"]
        assert "duplicate_session_id" in validated[1]["_validation_flags"]

    def test_validate_flags_empty_string_required_field(self, raw_sessions):
        """An empty or whitespace-only required field should be flagged."""
        rec = dict(raw_sessions[0])
        rec["stationID"] = "   "
        validated, _summary = validate_sessions([rec])
        assert "missing_required_field:stationID" in validated[0]["_validation_flags"]

    def test_validate_user_inputs_negative_values(self, raw_sessions):
        """Negative values in userInputs should trigger validation flags."""
        rec = dict(raw_sessions[0])
        rec["userInputs"] = [
            {
                "WhPerMile": -250,
                "kWhRequested": -10.0,
                "milesRequested": -50,
                "minutesAvailable": -120,
            }
        ]
        validated, _summary = validate_sessions([rec])
        flags = validated[0]["_validation_flags"]
        assert "user_input_negative:WhPerMile" in flags
        assert "user_input_negative:kWhRequested" in flags
        assert "user_input_negative:milesRequested" in flags
        assert "user_input_negative:minutesAvailable" in flags

    def test_validate_user_inputs_unparseable_timestamps(self, raw_sessions):
        """Unparseable timestamps in userInputs should trigger validation flags."""
        rec = dict(raw_sessions[0])
        rec["userInputs"] = [
            {
                "modifiedAt": "NOT_A_DATE",
                "requestedDeparture": "INVALID_TS",
            }
        ]
        validated, _summary = validate_sessions([rec])
        flags = validated[0]["_validation_flags"]
        assert "user_input_unparseable_timestamp:modifiedAt" in flags
        assert "user_input_unparseable_timestamp:requestedDeparture" in flags

    def test_validate_user_inputs_malformed_structure(self, raw_sessions):
        """Non-list userInputs should be flagged as malformed."""
        rec = dict(raw_sessions[0])
        rec["userInputs"] = "not_a_list"
        validated, _summary = validate_sessions([rec])
        assert "malformed_user_inputs" in validated[0]["_validation_flags"]


# ======================================================================
# TRANSFORM TESTS
# ======================================================================


class TestTransform:
    """Tests for ingestion.transform."""

    @pytest.fixture()
    def transformed(self):
        sessions, _meta = extract_sessions(SAMPLE_PATH)
        validated, _summary = validate_sessions(sessions)
        return transform_sessions(validated)

    def test_transform_returns_dataframes(self, transformed):
        """Should return two DataFrames."""
        sess_df, ui_df = transformed
        assert isinstance(sess_df, pd.DataFrame)
        assert isinstance(ui_df, pd.DataFrame)

    def test_transform_session_row_count(self, transformed):
        """All 6 sessions should be in the output."""
        sess_df, _ui_df = transformed
        assert len(sess_df) == 6

    def test_transform_timestamps_to_iso(self, transformed):
        """Timestamps should be ISO 8601 format."""
        sess_df, _ui_df = transformed
        conn = sess_df.iloc[0]["connection_time"]
        assert isinstance(conn, str)
        assert "T" in conn
        assert "+" in conn or "Z" in conn

    def test_transform_computed_durations(self, transformed):
        """Record 0: connected 17:41:45 to 21:00:00 = 198.25 min."""
        sess_df, _ui_df = transformed
        dur = sess_df.iloc[0]["session_duration_min"]
        assert dur is not None
        assert abs(dur - 198.25) < 0.1

    def test_transform_handles_null_done_charging(self, transformed):
        """Record 2 has null doneChargingTime — durations should be None."""
        sess_df, _ui_df = transformed
        rec = sess_df.iloc[2]
        assert rec["done_charging_time"] is None or pd.isna(rec["done_charging_time"])
        assert rec["charging_duration_min"] is None or pd.isna(rec["charging_duration_min"])
        assert rec["idle_duration_min"] is None or pd.isna(rec["idle_duration_min"])

    def test_transform_nulls_negative_idle_duration_on_telemetry_lag(self, transformed):
        """Record 4 (done > disconnect): idle_duration_min must be None (never negative)."""
        sess_df, _ui_df = transformed
        rec = sess_df.iloc[4]
        # Durations must not be negative
        assert rec["idle_duration_min"] is None or pd.isna(rec["idle_duration_min"])
        assert rec["charging_duration_min"] is None or pd.isna(rec["charging_duration_min"])
        # Validation flag explains why duration is uncalculated
        assert "done_after_disconnect" in rec["validation_flags"]

    def test_transform_handles_dst_outlier(self):
        """Simulate DST 1-hour outlier: idle_duration_min must be None."""
        raw_rec = {
            "_id": "dst_outlier_test",
            "sessionID": "test_dst_session",
            "siteID": "0002",
            "clusterID": "0039",
            "stationID": "2-39-89-25",
            "spaceID": "CA-315",
            "connectionTime": "Sun, 10 Mar 2019 03:51:37 GMT",
            "disconnectTime": "Sun, 10 Mar 2019 05:07:16 GMT",
            "doneChargingTime": "Sun, 10 Mar 2019 06:07:12 GMT",
            "kWhDelivered": 4.509,
            "userID": "000001135",
            "timezone": "America/Los_Angeles",
            "userInputs": None,
        }
        validated, _summary = validate_sessions([raw_rec])
        sess_df, _ = transform_sessions(validated)
        rec = sess_df.iloc[0]
        assert rec["idle_duration_min"] is None or pd.isna(rec["idle_duration_min"])
        assert "done_after_disconnect_outlier" in rec["validation_flags"]

    def test_transform_flattens_user_inputs(self, transformed):
        """User inputs should be in the second DataFrame."""
        _sess_df, ui_df = transformed
        assert len(ui_df) == 5
        assert "session_id" in ui_df.columns
        assert "kwh_requested" in ui_df.columns
        assert "wh_per_mile" in ui_df.columns

    def test_transform_snake_case_columns(self, transformed):
        """All session columns should be snake_case."""
        sess_df, _ui_df = transformed
        for col in sess_df.columns:
            assert col == col.lower(), f"Column '{col}' is not lowercase"
            assert " " not in col, f"Column '{col}' contains spaces"

    def test_transform_deterministic_with_fixed_execution_time(self):
        """Passing fixed execution_time must produce identical ingestion_timestamp."""
        sessions, _meta = extract_sessions(SAMPLE_PATH)
        validated, _ = validate_sessions(sessions)
        fixed_ts = "2026-01-01T12:00:00+00:00"
        df1, _ = transform_sessions(validated, execution_time=fixed_ts)
        df2, _ = transform_sessions(validated, execution_time=fixed_ts)
        assert (df1["ingestion_timestamp"] == fixed_ts).all()
        assert df1["ingestion_timestamp"].equals(df2["ingestion_timestamp"])

    def test_transform_anonymous_session(self, transformed):
        """Record 3 has null userID — should be preserved as None/NaN."""
        sess_df, _ui_df = transformed
        rec = sess_df.iloc[3]
        assert rec["user_id"] is None or pd.isna(rec["user_id"])


# ======================================================================
# LOAD TESTS
# ======================================================================


class TestLoad:
    """Tests for ingestion.load."""

    @pytest.fixture()
    def load_output(self, tmp_path):
        sessions, meta = extract_sessions(SAMPLE_PATH)
        validated, summary = validate_sessions(sessions)
        sess_df, ui_df = transform_sessions(validated)
        results = load_sessions(sess_df, ui_df, summary, meta, str(tmp_path))
        return results, tmp_path

    def test_load_creates_output_files(self, load_output):
        """All expected output files should exist."""
        results, tmp_path = load_output
        assert (tmp_path / "sessions.csv").exists()
        assert (tmp_path / "sessions.jsonl").exists()
        assert (tmp_path / "user_inputs.csv").exists()
        assert (tmp_path / "quality_report.json").exists()

    def test_load_csv_row_count(self, load_output):
        """CSV should have 6 data rows (+ header)."""
        _results, tmp_path = load_output
        lines = (tmp_path / "sessions.csv").read_text().strip().split("\n")
        assert len(lines) == 7  # 1 header + 6 data rows

    def test_load_jsonl_row_count(self, load_output):
        """JSONL should have 6 lines."""
        _results, tmp_path = load_output
        lines = (tmp_path / "sessions.jsonl").read_text().strip().split("\n")
        assert len(lines) == 6

    def test_load_quality_report_valid_json(self, load_output):
        """quality_report.json should contain extraction, validation, and output."""
        _results, tmp_path = load_output
        report = json.loads((tmp_path / "quality_report.json").read_text())
        assert "extraction" in report
        assert "validation" in report
        assert "source_metadata" in report
        assert "output" in report

    def test_load_results_summary(self, load_output):
        """Results dict should report correct row counts."""
        results, _tmp_path = load_output
        assert results["files"]["sessions.csv"]["rows"] == 6
        assert results["files"]["user_inputs.csv"]["rows"] == 5

    def test_load_csv_preserves_identifiers(self, load_output):
        """CSV roundtrip must preserve string format for site_id ('0002') and user_id ('000000101')."""
        _results, tmp_path = load_output
        # Read back with dtype=str to verify quotes preserved leading zeros
        df_read = pd.read_csv(tmp_path / "sessions.csv", dtype=str)
        assert df_read.iloc[0]["site_id"] == "0002"
        assert df_read.iloc[0]["cluster_id"] == "0039"
        # Record 0 in sample fixture has user_id = "000000101"
        assert df_read.iloc[0]["user_id"] == "000000101"


# ======================================================================
# END-TO-END PIPELINE TEST
# ======================================================================


class TestPipelineEndToEnd:
    """End-to-end tests using sample data."""

    def test_pipeline_end_to_end(self, tmp_path):
        """Full pipeline should run without errors on sample data."""
        results = run_pipeline(
            source_path=SAMPLE_PATH,
            output_dir=str(tmp_path),
        )

        assert "extract" in results["stages"]
        assert "validate" in results["stages"]
        assert "transform" in results["stages"]
        assert "load" in results["stages"]

        assert (tmp_path / "sessions.csv").exists()
        assert (tmp_path / "sessions.jsonl").exists()
        assert (tmp_path / "user_inputs.csv").exists()
        assert (tmp_path / "quality_report.json").exists()

        assert results["total_elapsed_sec"] > 0
        assert "completeness_status" in results

    def test_pipeline_deterministic_with_fixed_clock(self, tmp_path):
        """Two runs with the same execution_time produce identical sessions.csv."""
        dir1 = tmp_path / "run1"
        dir2 = tmp_path / "run2"
        fixed_clock = "2026-02-01T00:00:00+00:00"

        run_pipeline(SAMPLE_PATH, output_dir=str(dir1), execution_time=fixed_clock)
        run_pipeline(SAMPLE_PATH, output_dir=str(dir2), execution_time=fixed_clock)

        csv1 = (dir1 / "sessions.csv").read_text(encoding="utf-8")
        csv2 = (dir2 / "sessions.csv").read_text(encoding="utf-8")
        assert csv1 == csv2
