"""Focused tests for Kafka event publishing."""

import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

from streaming.producer import (
    TrackedKafkaProducer,
    _load_project_environment,
    create_producer,
    flush_producer,
    main,
    publish_event,
)


class FakeProducer:
    def __init__(self, pending=0):
        self.messages = []
        self.pending = pending
        self.poll_timeouts = []
        self.flush_timeouts = []
        self.delivery_errors = []

    def produce(self, topic, **kwargs):
        self.messages.append((topic, kwargs))

    def poll(self, timeout):
        self.poll_timeouts.append(timeout)
        return 0

    def flush(self, timeout=10):
        self.flush_timeouts.append(timeout)
        return self.pending


def sample_event():
    return {
        "event_id": "87f3a2a0-8471-4568-9928-708c6ec7b42d",
        "station_id": "2-39-81-4550",
        "session_id": "test-session",
        "event_type": "session_start",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "energy_kwh": 0.0,
        "incremental_kwh": 0.0,
    }


class ProducerTests(unittest.TestCase):
    def test_project_environment_loads_repository_root_dotenv(self):
        with patch("streaming.producer.load_dotenv") as load_dotenv:
            _load_project_environment()

        load_dotenv.assert_called_once_with(
            dotenv_path=Path(__file__).resolve().parents[1] / ".env"
        )

    def test_create_producer_uses_process_configuration(self):
        raw_producer = FakeProducer()
        kafka_module = types.ModuleType("confluent_kafka")
        kafka_module.Producer = Mock(return_value=raw_producer)
        with (
            patch.dict(
                sys.modules,
                {"confluent_kafka": kafka_module},
            ),
            patch.dict(
                os.environ,
                {
                    "KAFKA_BOOTSTRAP_SERVERS": "broker.example:29092",
                    "KAFKA_CLIENT_ID": "streaming-test",
                },
            ),
        ):
            producer = create_producer()

        self.assertIsInstance(producer, TrackedKafkaProducer)
        kafka_module.Producer.assert_called_once_with(
            {
                "bootstrap.servers": "broker.example:29092",
                "client.id": "streaming-test",
            }
        )

    def test_publish_event_sends_json_to_configured_topic(self):
        producer = FakeProducer()
        with patch.dict(
            os.environ,
            {"KAFKA_TOPIC_CHARGING_EVENTS": "test-charging-events"},
        ):
            publish_event(producer, sample_event())

        topic, message = producer.messages[0]
        self.assertEqual(topic, "test-charging-events")
        self.assertEqual(message["key"], "test-session")
        self.assertEqual(json.loads(message["value"]), sample_event())
        self.assertTrue(callable(message["callback"]))
        self.assertEqual(producer.poll_timeouts, [0])

    def test_publish_event_uses_default_topic_when_not_configured(self):
        producer = FakeProducer()
        with patch.dict(os.environ, {}, clear=True), patch(
            "streaming.producer.KAFKA_TOPIC",
            "charging-events",
        ):
            publish_event(producer, sample_event())

        self.assertEqual(producer.messages[0][0], "charging-events")

    def test_publish_event_rejects_unsupported_event_type(self):
        event = sample_event()
        event["event_type"] = "heartbeat"

        with self.assertRaisesRegex(ValueError, "Unsupported event_type"):
            publish_event(FakeProducer(), event)

    def test_publish_event_accepts_plain_and_prefixed_uuid4(self):
        for event_id in (
            "87f3a2a0-8471-4568-9928-708c6ec7b42d",
            "evt-87f3a2a0-8471-4568-9928-708c6ec7b42d",
        ):
            with self.subTest(event_id=event_id):
                event = sample_event()
                event["event_id"] = event_id
                publish_event(FakeProducer(), event)

    def test_publish_event_rejects_invalid_uuid_and_uuid_version(self):
        for event_id in (
            "not-a-uuid",
            "87f3a2a0-8471-1568-9928-708c6ec7b42d",
            "evt-87f3a2a0-8471-1568-9928-708c6ec7b42d",
            "EVT-87f3a2a0-8471-4568-9928-708c6ec7b42d",
        ):
            with self.subTest(event_id=event_id):
                event = sample_event()
                event["event_id"] = event_id
                with self.assertRaisesRegex(ValueError, "UUID4"):
                    publish_event(FakeProducer(), event)

    def test_publish_event_rejects_missing_and_unsupported_fields(self):
        event = sample_event()
        del event["station_id"]
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            publish_event(FakeProducer(), event)

        event = sample_event()
        event["unexpected"] = "value"
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            publish_event(FakeProducer(), event)

    def test_publish_event_rejects_invalid_and_overlength_fields(self):
        invalid_events = []
        for field, value in (
            ("event_id", "a" * 65),
            ("station_id", "s" * 21),
            ("session_id", "s" * 65),
            ("event_type", "x" * 21),
            ("station_id", ""),
            ("session_id", 123),
        ):
            event = sample_event()
            event[field] = value
            invalid_events.append((field, event))

        for field, event in invalid_events:
            with self.subTest(field=field, value=event[field]):
                with self.assertRaises(ValueError):
                    publish_event(FakeProducer(), event)

    def test_incremental_energy_is_optional_and_validated_when_present(self):
        event = sample_event()
        del event["incremental_kwh"]
        publish_event(FakeProducer(), event)

        event["incremental_kwh"] = 0.0
        publish_event(FakeProducer(), event)
        event["incremental_kwh"] = 0.1
        with self.assertRaisesRegex(ValueError, "session_start"):
            publish_event(FakeProducer(), event)

    def test_publish_event_rejects_invalid_energy_values(self):
        for field in ("energy_kwh", "incremental_kwh"):
            for value in (-1, True, float("nan"), float("inf"), float("-inf")):
                with self.subTest(field=field, value=value):
                    event = sample_event()
                    event[field] = value
                    with self.assertRaisesRegex(ValueError, "non-negative"):
                        publish_event(FakeProducer(), event)

    def test_publish_event_accepts_utc_timestamp_and_rejects_invalid_timestamps(self):
        event = sample_event()
        event["timestamp"] = "2026-01-01T00:00:00Z"
        publish_event(FakeProducer(), event)

        for timestamp in (
            "2026-01-01T00:00:00",
            "2026-01-01T00:00:00-08:00",
            "not-a-timestamp",
        ):
            with self.subTest(timestamp=timestamp):
                event = sample_event()
                event["timestamp"] = timestamp
                with self.assertRaisesRegex(ValueError, "UTC"):
                    publish_event(FakeProducer(), event)

    def test_delivery_callback_success_and_flush(self):
        underlying = FakeProducer()
        producer = TrackedKafkaProducer(underlying)
        publish_event(producer, sample_event())
        callback = underlying.messages[0][1]["callback"]

        callback(None, FakeKafkaMessage())
        flush_producer(producer)

        self.assertEqual(producer.delivery_errors, [])
        self.assertEqual(underlying.flush_timeouts, [10.0])

    def test_delivery_callback_failure_is_reported_after_flush(self):
        underlying = FakeProducer()
        producer = TrackedKafkaProducer(underlying)
        publish_event(producer, sample_event())
        callback = underlying.messages[0][1]["callback"]

        callback("broker rejected message", FakeKafkaMessage())
        with self.assertRaisesRegex(RuntimeError, "broker rejected message"):
            flush_producer(producer)
        self.assertEqual(producer.delivery_errors, ["broker rejected message"])

    def test_flush_reports_pending_messages(self):
        producer = TrackedKafkaProducer(FakeProducer(pending=1))
        with self.assertRaisesRegex(TimeoutError, "1 message\\(s\\) pending"):
            flush_producer(producer)
        self.assertEqual(producer._producer.flush_timeouts, [10.0])

    def test_flush_reports_pending_messages_and_delivery_failures(self):
        producer = TrackedKafkaProducer(FakeProducer(pending=1))
        producer.delivery_errors.append("broker rejected message")

        with self.assertRaisesRegex(TimeoutError, "1 delivery failure"):
            flush_producer(producer)

    def test_main_preserves_publish_error_when_shutdown_flush_fails(self):
        producer = TrackedKafkaProducer(FakeProducer())
        publish_error = ValueError("publish failed")
        flush_error = TimeoutError("flush timed out")

        with (
            patch("streaming.producer.create_producer", return_value=producer),
            patch("streaming.producer.stream_events", return_value=[sample_event()]),
            patch("streaming.producer.publish_event", side_effect=publish_error),
            patch("streaming.producer.flush_producer", side_effect=flush_error) as flush,
        ):
            with self.assertRaisesRegex(ValueError, "publish failed") as raised:
                main()

        self.assertIs(raised.exception, publish_error)
        self.assertIs(raised.exception.__cause__, flush_error)
        flush.assert_called_once_with(producer)


class FakeKafkaMessage:
    def topic(self):
        return "charging-events"

    def partition(self):
        return 0

    def offset(self):
        return 1


if __name__ == "__main__":
    unittest.main()
