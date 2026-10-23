"""
tests/test_dashboard.py — Unit tests for Streamlit CHARGE INTELLIGENCE components.

Owner: Shipra Choubey
"""

import pandas as pd
import pytest

from dashboard.components.data_loader import (
    load_sessions_data,
    load_user_inputs_data,
    load_pipeline_audit_reports,
    check_postgres_status,
)
from dashboard.components.insights import generate_executive_insights
from dashboard.components.charts import (
    plot_energy_and_volume_timeline,
    plot_station_comparison_bar,
    plot_demand_heatmap,
    plot_station_performance_quadrant,
    plot_session_duration_distribution,
    plot_energy_per_session_comparison,
    plot_statistical_anomaly_timeline,
    plot_quality_audit_breakdown,
    plot_missing_values_bar,
    plot_driver_preferences,
)


def test_load_sessions_data_schema():
    """Verify that sessions data loader loads valid DataFrame with required schema columns."""
    df, meta = load_sessions_data()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    expected_cols = {
        "session_id", "station_id", "kwh_delivered",
        "session_duration_min", "connection_time"
    }
    assert expected_cols.issubset(set(df.columns))
    assert "is_clean" in df.columns
    assert "connection_date_local" in df.columns
    assert meta["row_count"] == len(df)


def test_load_user_inputs_data():
    """Verify that user inputs data loader loads valid DataFrame with preferences."""
    df, meta = load_user_inputs_data()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    expected_cols = {"session_id", "kwh_requested", "wh_per_mile"}
    assert expected_cols.issubset(set(df.columns))


def test_load_pipeline_audit_reports():
    """Verify that pipeline audit report loader loads dictionary structure."""
    reports = load_pipeline_audit_reports()
    assert isinstance(reports, dict)
    assert "quality_report" in reports
    assert "pipeline_log_tail" in reports


def test_check_postgres_status():
    """Verify that postgres status check executes safely without raising exceptions."""
    status = check_postgres_status()
    assert isinstance(status, dict)
    assert "configured" in status
    assert "connected" in status


def test_generate_executive_insights():
    """Verify dynamic insight generation produces structured, non-empty observations."""
    df, _ = load_sessions_data()
    reports = load_pipeline_audit_reports()
    insights = generate_executive_insights(df, reports.get("quality_report"))
    assert isinstance(insights, list)
    assert len(insights) > 0
    for ins in insights:
        assert "category" in ins
        assert "title" in ins
        assert "body" in ins
        assert "type" in ins


def test_advanced_charts_generation():
    """Ensure all advanced Plotly visualization routines produce valid figures."""
    df, _ = load_sessions_data()
    user_df, _ = load_user_inputs_data()

    fig_timeline = plot_energy_and_volume_timeline(df)
    assert fig_timeline is not None

    fig_bar = plot_station_comparison_bar(df, metric="energy")
    assert fig_bar is not None

    fig_heat = plot_demand_heatmap(df, metric="sessions", use_local_tz=True)
    assert fig_heat is not None

    fig_quad = plot_station_performance_quadrant(df)
    assert fig_quad is not None

    fig_dur = plot_session_duration_distribution(df)
    assert fig_dur is not None

    fig_per_session = plot_energy_per_session_comparison(df)
    assert fig_per_session is not None

    fig_spike = plot_statistical_anomaly_timeline(df)
    assert fig_spike is not None

    fig_quality = plot_quality_audit_breakdown(df)
    assert fig_quality is not None

    fig_nulls = plot_missing_values_bar(df)
    assert fig_nulls is not None

    fig_driver = plot_driver_preferences(user_df)
    assert fig_driver is not None


def test_observatory_system_health():
    """Verify that streaming system health check executes cleanly without errors."""
    from dashboard.components.observatory import get_streaming_system_health, check_port_open
    health = get_streaming_system_health()
    assert isinstance(health, dict)
    assert "kafka_online" in health
    assert "topic" in health
    assert health["topic"] == "charging-events"
    # Verify port checker doesn't throw unhandled errors
    assert isinstance(check_port_open("localhost", 9999, timeout=0.1), bool)

