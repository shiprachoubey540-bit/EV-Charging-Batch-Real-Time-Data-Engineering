"""
streaming/kafka_smoke_test.py — Helper to verify Kafka broker connectivity.

Owner: Harikrishnan SM / Team
"""

import socket


def check_kafka_connectivity(broker_address: str = "localhost:9092", timeout_seconds: float = 1.0) -> bool:
    """
    Check if a Kafka broker is reachable via TCP socket.

    Parameters
    ----------
    broker_address : str
        Host and port of the Kafka broker (default: "localhost:9092").
    timeout_seconds : float
        Connection timeout in seconds.

    Returns
    -------
    bool
        True if connection succeeded, False otherwise.
    """
    try:
        if ":" in broker_address:
            host, port_str = broker_address.split(":", 1)
            port = int(port_str)
        else:
            host = broker_address
            port = 9092
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False
