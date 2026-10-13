"""Tests for the ingestion module.

Uses the sample fixture at ``data/sample/sample_sessions.json`` which
contains 6 records covering normal, null, and anomaly cases:
    - Record 0: normal record with userInputs
    - Record 1: normal record with userInputs
    - Record 2: doneChargingTime = null (null done charging)
    - Record 3: userID = null, userInputs = null (anonymous session)
    - Record 4: doneChargingTime > disconnectTime (anomaly)
    - Record 5: normal record, same user as record 0
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from ingestion.extract import extract_sessions, _repair_truncated_json
from ingestion.validate import validate_sessions
from ingestion.transform import transform_sessions
from ingestion.load import load_sessions

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

    def test_extract_returns_metadata(self):
        """_meta should contain site, start, end keys."""
        _sessions, meta = extract_sessions(SAMPLE_PATH)
        assert meta.get("site") == "caltech"
        assert "start" in meta
        assert "end" in meta

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
        # Build a truncated JSON string
        truncated = '{"_meta": {}, "_items": [{"a": 1}, {"a": 2},'
        repaired = _repair_truncated_json(truncated)
        data = json.loads(repaired)
        assert data["_items"] == [{"a": 1}, {"a": 2}]

    def test_extract_valid_json_passes_through(self):
        """Already-valid JSON should not be altered."""
        valid = '{"_meta": {}, "_items": [{"a": 1}]}'
        result = _repair_truncated_json(valid)
        assert json.loads(result) == json.loads(valid)

    def test_extract_file_not_found(self):
        """Non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            extract_sessions("/nonexistent/path.json")


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
        # Record 4 (index 4) is the anomaly record
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
        # Records 0 and 1 are normal
        assert validated[0]["_validation_flags"] == []
        assert validated[1]["_validation_flags"] == []

    def test_validate_summary_counts(self, raw_sessions):
        """Summary should report correct total and flagged counts."""
        _validated, summary = validate_sessions(raw_sessions)
        assert summary["total_records"] == 6
        assert summary["flagged_records"] >= 2  # At least records 2 and 4


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
        import pandas as pd
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
        # ISO 8601 should contain 'T' separator and '+' timezone
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
        import pandas as pd
        rec = sess_df.iloc[2]
        assert rec["done_charging_time"] is None or pd.isna(rec["done_charging_time"])
        assert rec["charging_duration_min"] is None or pd.isna(rec["charging_duration_min"])
        assert rec["idle_duration_min"] is None or pd.isna(rec["idle_duration_min"])

    def test_transform_flattens_user_inputs(self, transformed):
        """User inputs should be in the second DataFrame."""
        _sess_df, ui_df = transformed
        # Records 0, 1, 2, 4, 5 have userInputs; record 3 does not
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

    def test_transform_ingestion_timestamp_present(self, transformed):
        """Every row should have a non-null ingestion_timestamp."""
        sess_df, _ui_df = transformed
        assert sess_df["ingestion_timestamp"].notna().all()

    def test_transform_anonymous_session(self, transformed):
        """Record 3 has null userID — should be preserved as None/NaN."""
        import pandas as pd
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
        """quality_report.json should be valid JSON."""
        _results, tmp_path = load_output
        report = json.loads((tmp_path / "quality_report.json").read_text())
        assert "validation" in report
        assert "source_metadata" in report

    def test_load_results_summary(self, load_output):
        """Results dict should report correct row counts."""
        results, _tmp_path = load_output
        assert results["files"]["sessions.csv"]["rows"] == 6
        assert results["files"]["user_inputs.csv"]["rows"] == 5


# ======================================================================
# END-TO-END PIPELINE TEST
# ======================================================================


class TestPipelineEndToEnd:
    """End-to-end test using sample data."""

    def test_pipeline_end_to_end(self, tmp_path):
        """Full pipeline should run without errors on sample data."""
        from ingestion.run_pipeline import run_pipeline

        results = run_pipeline(
            source_path=SAMPLE_PATH,
            output_dir=str(tmp_path),
        )

        # Check all stages ran
        assert "extract" in results["stages"]
        assert "validate" in results["stages"]
        assert "transform" in results["stages"]
        assert "load" in results["stages"]

        # Check outputs
        assert (tmp_path / "sessions.csv").exists()
        assert (tmp_path / "sessions.jsonl").exists()
        assert (tmp_path / "user_inputs.csv").exists()
        assert (tmp_path / "quality_report.json").exists()

        # Check timing
        assert results["total_elapsed_sec"] > 0
