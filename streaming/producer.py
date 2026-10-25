"""
streaming/producer.py — Kafka message producer for charging events.

Owner: Harikrishnan SM
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CHARGING_EVENTS", "charging-events")


def create_producer(config: Optional[Dict[str, Any]] = None):
    """
    Create and return a Kafka producer instance.

    Parameters
    ----------
    config : Optional[Dict[str, Any]]
        Optional overrides for Kafka producer configuration.

    Returns
    -------
    confluent_kafka.Producer
        Configured Kafka producer.
    """
    try:
        from confluent_kafka import Producer
    except ImportError:
        raise ImportError("confluent-kafka is required to create a Kafka producer")

    default_config: Dict[str, Any] = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "ev-charging-stream-producer",
        "acks": "all",
    }
    if config:
        default_config.update(config)

    return Producer(default_config)


def publish_event(producer, event: dict, topic: Optional[str] = None) -> None:
    """
    Serialize and publish a single event to the Kafka topic.

    Parameters
    ----------
    producer : confluent_kafka.Producer
        Active Kafka producer.
    event : dict
        Event record to publish.
    topic : Optional[str]
        Target Kafka topic (defaults to KAFKA_TOPIC).
    """
    target_topic = topic or KAFKA_TOPIC
    key = str(event.get("station_id") or event.get("event_id") or "").encode("utf-8")
    payload = json.dumps(event).encode("utf-8")

    def delivery_report(err, msg):
        if err is not None:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.debug(
                "Message delivered to %s [%s] at offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    producer.produce(
        topic=target_topic,
        key=key,
        value=payload,
        callback=delivery_report,
    )
    producer.poll(0)


if __name__ == "__main__":
    from streaming.event_generator import stream_events

    producer = create_producer()
    for event in stream_events():
        publish_event(producer, event)
