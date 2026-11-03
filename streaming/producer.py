"""Publish simulated EV charging events to Kafka.

Owner: Harikrishnan SM
"""

from __future__ import annotations

import json
import logging
import math
import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID

from dotenv import load_dotenv

from streaming.event_generator import stream_events

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_project_environment() -> None:
    load_dotenv(dotenv_path=_PROJECT_ROOT / ".env")


_load_project_environment()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CHARGING_EVENTS", "charging-events")

_REQUIRED_EVENT_FIELDS = {
    "event_id",
    "station_id",
    "session_id",
    "event_type",
    "timestamp",
    "energy_kwh",
}
_ALLOWED_EVENT_FIELDS = _REQUIRED_EVENT_FIELDS | {"incremental_kwh"}
_ALLOWED_EVENT_TYPES = {"session_start", "energy_update", "session_end"}


class KafkaClient(Protocol):
    """Kafka client methods used to enqueue, poll, and flush messages."""

    def produce(self, topic: str, **kwargs: object) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...


class KafkaProducer(KafkaClient, Protocol):
    """Kafka client that tracks asynchronous delivery failures."""

    delivery_errors: list[str]


class KafkaMessage(Protocol):
    """Delivery metadata used in producer logs."""

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


class TrackedKafkaProducer:
    """Record asynchronous delivery failures from a Confluent producer."""

    def __init__(self, producer: KafkaClient) -> None:
        self._producer = producer
        self.delivery_errors: list[str] = []

    def produce(self, topic: str, **kwargs: object) -> None:
        callback = kwargs.get("callback")
        if callback is not None:
            if not callable(callback):
                raise TypeError("Kafka delivery callback must be callable.")
            original_callback = callback

            def track_delivery(error: object, message: KafkaMessage) -> None:
                if error is not None:
                    self.delivery_errors.append(str(error))
                original_callback(error, message)

            kwargs["callback"] = track_delivery
        self._producer.produce(topic, **kwargs)

    def poll(self, timeout: float) -> int:
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)


def create_producer(
    config: Mapping[str, object] | None = None,
) -> TrackedKafkaProducer:
    """Create a configured Confluent Kafka producer, applying optional overrides."""
    try:
        from confluent_kafka import Producer
    except ImportError as error:
        raise ImportError(
            "Kafka publishing requires confluent-kafka; install "
            "streaming/requirements.txt in the active virtual environment."
        ) from error

    producer_config: dict[str, object] = {
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            KAFKA_BOOTSTRAP_SERVERS,
        ),
        "client.id": os.getenv(
            "KAFKA_CLIENT_ID",
            "ev-charging-stream-producer",
        ),
        "acks": "all",
    }
    if config:
        producer_config.update(config)
    return TrackedKafkaProducer(Producer(producer_config))


def _delivery_report(error: object, message: KafkaMessage) -> None:
    if error is not None:
        logger.error("Kafka delivery failed: %s", error)
        return
    logger.debug(
        "Kafka event delivered to %s [%s] at offset %s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def _validate_event(event: Mapping[str, object]) -> None:
    missing_fields = _REQUIRED_EVENT_FIELDS - event.keys()
    unexpected_fields = event.keys() - _ALLOWED_EVENT_FIELDS
    if missing_fields:
        raise ValueError(f"Event is missing required fields: {sorted(missing_fields)}")
    if unexpected_fields:
        raise ValueError(f"Event has unsupported fields: {sorted(unexpected_fields)}")
    event_type = event["event_type"]
    if not isinstance(event_type, str):
        raise ValueError("event_type must be a string.")
    for field in ("event_id", "station_id", "session_id", "event_type", "timestamp"):
        value = event[field]
        if not isinstance(value, str) or not value:
            raise ValueError("Event IDs and timestamp must be non-empty strings.")
    field_lengths = {
        "event_id": 64,
        "station_id": 20,
        "session_id": 64,
        "event_type": 20,
    }
    for field, maximum_length in field_lengths.items():
        value = event[field]
        if isinstance(value, str) and len(value) > maximum_length:
            raise ValueError(f"{field} must not exceed {maximum_length} characters.")
    if event_type not in _ALLOWED_EVENT_TYPES:
        raise ValueError(f"Unsupported event_type: {event_type!r}")

    event_id = event["event_id"]
    if not isinstance(event_id, str):
        raise ValueError("event_id must be a string.")
    uuid_value = event_id[4:] if event_id.startswith("evt-") else event_id
    try:
        event_uuid = UUID(uuid_value)
    except ValueError as error:
        raise ValueError(
            "event_id must be a canonical UUID4, optionally prefixed with 'evt-'."
        ) from error
    if event_uuid.version != 4 or str(event_uuid) != uuid_value:
        raise ValueError(
            "event_id must be a canonical UUID4, optionally prefixed with 'evt-'."
        )
    timestamp = event["timestamp"]
    if not isinstance(timestamp, str):
        raise ValueError("timestamp must be an ISO 8601 UTC timestamp.")
    try:
        event_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("timestamp must be an ISO 8601 UTC timestamp.") from error
    if (
        "T" not in timestamp
        or event_timestamp.utcoffset() is None
        or event_timestamp.utcoffset().total_seconds()
    ):
        raise ValueError("timestamp must be an ISO 8601 UTC timestamp.")

    energy = event["energy_kwh"]
    if not _is_nonnegative_finite_number(energy):
        raise ValueError("energy_kwh must be a non-negative number.")
    if "incremental_kwh" in event:
        incremental_energy = event["incremental_kwh"]
        if not _is_nonnegative_finite_number(incremental_energy):
            raise ValueError("incremental_kwh must be a non-negative number.")
    if event_type == "session_start" and (
        energy != 0
        or ("incremental_kwh" in event and event["incremental_kwh"] != 0)
    ):
        raise ValueError("session_start events must have zero energy.")


def _is_nonnegative_finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def publish_event(
    producer: KafkaProducer,
    event: Mapping[str, object],
    topic: str | None = None,
) -> None:
    """Validate, serialize, and enqueue one event for Kafka delivery."""
    _validate_event(event)
    message_topic = topic or os.getenv("KAFKA_TOPIC_CHARGING_EVENTS", KAFKA_TOPIC)
    payload = json.dumps(event, allow_nan=False).encode("utf-8")
    producer.produce(
        topic=message_topic,
        key=str(event["station_id"]).encode("utf-8"),
        value=payload,
        callback=_delivery_report,
    )
    producer.poll(0)


def flush_producer(producer: KafkaProducer, timeout: float = 10.0) -> None:
    """Wait for queued messages and report if the timeout leaves any pending."""
    pending_messages = producer.flush(timeout)
    delivery_errors = producer.delivery_errors
    if pending_messages or delivery_errors:
        details = []
        if pending_messages:
            details.append(f"{pending_messages} message(s) pending")
        if delivery_errors:
            details.append(
                f"{len(delivery_errors)} delivery failure(s): "
                + "; ".join(delivery_errors)
            )
        message = "Kafka flush did not complete successfully: " + "; ".join(details)
        if pending_messages:
            raise TimeoutError(message)
        raise RuntimeError(message)


def main() -> None:
    producer = create_producer()
    local_testing = os.getenv("EV_CHARGING_LOCAL_TESTING", "").lower() in {
        "1",
        "true",
        "yes",
    }
    try:
        for event in stream_events(local_testing=local_testing):
            publish_event(producer, event)
    except KeyboardInterrupt as stopping_error:
        logger.info("Stopping Kafka event production.")
        try:
            flush_producer(producer)
        except Exception as flush_error:
            raise stopping_error from flush_error
    except Exception as publishing_error:
        try:
            flush_producer(producer)
        except Exception as flush_error:
            raise publishing_error from flush_error
        raise
    else:
        flush_producer(producer)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
