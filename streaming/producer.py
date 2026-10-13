"""
streaming/producer.py — Kafka message producer for charging events.

Owner: Harikrishnan SM
"""

import json
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CHARGING_EVENTS", "charging-events")


def create_producer():
    """
    Create and return a Kafka producer instance.

    Returns
    -------
    confluent_kafka.Producer
        Configured Kafka producer.
    """
    # TODO: Implement with confluent_kafka.Producer
    raise NotImplementedError("Implement Kafka producer")


def publish_event(producer, event: dict) -> None:
    """
    Serialize and publish a single event to the Kafka topic.

    Parameters
    ----------
    producer : confluent_kafka.Producer
        Active Kafka producer.
    event : dict
        Event record to publish.
    """
    # TODO: Implement publishing logic
    raise NotImplementedError("Implement publish logic")


if __name__ == "__main__":
    from streaming.event_generator import stream_events

    producer = create_producer()
    for event in stream_events():
        publish_event(producer, event)
