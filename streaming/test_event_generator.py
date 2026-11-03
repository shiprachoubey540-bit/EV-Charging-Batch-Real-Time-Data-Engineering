"""Focused tests for the EV charging event generator."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as parquet

import streaming.event_generator as event_generator
from streaming.event_generator import (
    generate_session_events,
    resolve_station_ids,
    stream_events,
)

VERIFIED_STATION_ID = "2-39-81-4550"
TEST_STATION_IDS_PATH = (
    Path(event_generator.__file__).resolve().parent
    / "testdata"
    / "verified_station_ids.csv"
)


class EventGeneratorTests(unittest.TestCase):
    def test_session_events_follow_contract_lifecycle_and_energy_rules(self):
        events = list(
            generate_session_events(
                VERIFIED_STATION_ID,
                session_id="test-session",
                update_count=2,
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        )

        self.assertEqual(
            [event["event_type"] for event in events],
            ["session_start", "energy_update", "energy_update", "session_end"],
        )
        self.assertTrue(all(event["session_id"] == "test-session" for event in events))
        self.assertTrue(
            all(event["station_id"] == VERIFIED_STATION_ID for event in events)
        )
        self.assertEqual(len({event["event_id"] for event in events}), len(events))
        self.assertEqual(events[0]["energy_kwh"], 0.0)
        self.assertEqual(events[0]["incremental_kwh"], 0.0)
        self.assertTrue(
            all(
                current["energy_kwh"] >= previous["energy_kwh"]
                for previous, current in zip(events, events[1:])
            )
        )
        self.assertAlmostEqual(
            events[-1]["energy_kwh"],
            sum(event["incremental_kwh"] for event in events),
        )
        timestamps = [
            datetime.fromisoformat(str(event["timestamp"])) for event in events
        ]
        self.assertTrue(
            all(timestamp.utcoffset().total_seconds() == 0 for timestamp in timestamps)
        )
        self.assertEqual(timestamps, sorted(timestamps))

    def test_stream_starts_new_session_only_after_previous_ends(self):
        events = iter(
            stream_events(interval_seconds=0, station_ids=[VERIFIED_STATION_ID])
        )
        first_session = []
        while not first_session or first_session[-1]["event_type"] != "session_end":
            first_session.append(next(events))
        second_session_start = next(events)

        self.assertEqual(first_session[0]["event_type"], "session_start")
        self.assertEqual(second_session_start["event_type"], "session_start")
        self.assertNotEqual(
            first_session[-1]["session_id"],
            second_session_start["session_id"],
        )
        self.assertEqual(
            first_session[-1]["station_id"],
            second_session_start["station_id"],
        )

    def test_station_ids_load_from_sessions_parquet(self):
        station_ids = event_generator._read_station_ids_csv(TEST_STATION_IDS_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "sessions.parquet"
            parquet.write_table(
                pa.table({"station_id": [*station_ids, station_ids[0], None]}),
                parquet_path,
            )
            with patch.object(event_generator, "_SESSIONS_PARQUET_PATH", parquet_path):
                self.assertEqual(resolve_station_ids(), station_ids)
                self.assertEqual(len(station_ids), 55)
                self.assertEqual(station_ids[0], VERIFIED_STATION_ID)

    def test_stream_samples_only_from_verified_station_ids(self):
        station_ids = event_generator._read_station_ids_csv(TEST_STATION_IDS_PATH)
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "sessions.parquet"
            parquet.write_table(pa.table({"station_id": station_ids}), parquet_path)
            with patch.object(
                event_generator,
                "_SESSIONS_PARQUET_PATH",
                parquet_path,
            ):
                event = next(stream_events(interval_seconds=0))

        self.assertIn(event["station_id"], station_ids)

    def test_sessions_parquet_must_have_station_id_column(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "sessions.parquet"
            parquet.write_table(
                pa.table({"other_column": [VERIFIED_STATION_ID]}),
                parquet_path,
            )
            with patch.object(
                event_generator,
                "_SESSIONS_PARQUET_PATH",
                parquet_path,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "does not contain a station_id column",
                ):
                    resolve_station_ids()

    def test_station_ids_require_parquet_or_explicit_local_mode(self):
        with patch.object(
            event_generator,
            "_SESSIONS_PARQUET_PATH",
            Path(TEST_STATION_IDS_PATH.parent) / "missing.parquet",
        ):
            with self.assertRaisesRegex(
                FileNotFoundError,
                "Historical sessions Parquet file not found",
            ):
                resolve_station_ids()
            self.assertEqual(
                resolve_station_ids(local_testing=True),
                event_generator._read_station_ids_csv(TEST_STATION_IDS_PATH),
            )

    def test_explicit_station_ids_still_support_focused_tests(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "sessions.parquet"
            with patch.object(event_generator, "_SESSIONS_PARQUET_PATH", missing_path):
                self.assertEqual(
                    resolve_station_ids([VERIFIED_STATION_ID]),
                    (VERIFIED_STATION_ID,),
                )

    def test_duplicate_station_ids_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicates"):
            resolve_station_ids(["approved-1", "approved-1"])

    def test_configured_station_ids_require_nonempty_strings_of_at_most_20_chars(self):
        self.assertEqual(resolve_station_ids(["s" * 20]), ("s" * 20,))
        for station_ids in (
            [1],
            [""],
            ["   "],
            ["s" * 21],
            123,
        ):
            with self.subTest(station_ids=station_ids):
                with self.assertRaises(ValueError):
                    resolve_station_ids(station_ids)

    def test_generate_session_events_validates_station_id_type_and_length(self):
        events = list(generate_session_events("s" * 20, update_count=1))
        self.assertTrue(all(event["station_id"] == "s" * 20 for event in events))

        for station_id in (None, 1, "", "   ", "s" * 21):
            with self.subTest(station_id=station_id):
                with self.assertRaises(ValueError):
                    list(generate_session_events(station_id, update_count=1))

    def test_generate_session_events_validates_explicit_session_id(self):
        events = list(
            generate_session_events(
                VERIFIED_STATION_ID,
                session_id="s" * 64,
                update_count=1,
            )
        )
        self.assertTrue(all(event["session_id"] == "s" * 64 for event in events))

        for session_id in ("", "   ", 1, "s" * 65):
            with self.subTest(session_id=session_id):
                with self.assertRaises(ValueError):
                    list(
                        generate_session_events(
                            VERIFIED_STATION_ID,
                            session_id=session_id,
                            update_count=1,
                        )
                    )

    def test_negative_interval_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must not be negative"):
            next(stream_events(interval_seconds=-1, station_ids=[VERIFIED_STATION_ID]))


if __name__ == "__main__":
    unittest.main()
