"""Tests for the streaming module."""

from streaming.event_generator import generate_event


def test_generate_event_has_required_fields():
    """Each event must contain the fields defined in the data contract."""
    event = generate_event()
    required_fields = {"event_id", "station_id", "event_type", "timestamp", "energy_kwh"}
    assert required_fields.issubset(event.keys()), f"Missing fields: {required_fields - event.keys()}"


def test_generate_event_energy_range():
    """energy_kwh should be between 0 and 75."""
    event = generate_event()
    assert 0.0 <= event["energy_kwh"] <= 75.0
