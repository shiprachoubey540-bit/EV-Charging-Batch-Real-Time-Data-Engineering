"""Tests for the database module."""


def test_schema_file_exists():
    """database/schema.sql must exist."""
    import os
    assert os.path.isfile("database/schema.sql"), "schema.sql not found"
