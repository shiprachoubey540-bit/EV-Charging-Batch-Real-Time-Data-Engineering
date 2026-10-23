"""
dashboard/components/kpis.py — High-density industrial KPI cards.

Owner: Shipra Choubey

Visual Direction:
  Compact, typography-driven metric containers with precise units,
  subtle borders, and clear data definitions.
"""

from datetime import datetime, timezone
from typing import Any, Optional
import pandas as pd
import streamlit as st

from dashboard.components.theme import (
    COLOR_BG_CARD,
    COLOR_BORDER,
    COLOR_CYAN,
    COLOR_EMERALD,
    COLOR_AMBER,
    COLOR_SLATE,
    COLOR_MUTED,
)


def render_executive_kpis(df: pd.DataFrame) -> None:
    """
    Render 6 executive summary KPI tiles across a responsive grid.

    Parameters
    ----------
    df : pd.DataFrame
        Filtered charging sessions DataFrame.
    """
    if df.empty:
        st.info("No records match the selected date or station filter.")
        return

    total_sessions = len(df)
    total_energy_kwh = df["kwh_delivered"].sum() if "kwh_delivered" in df.columns else 0.0
    active_stations = df["station_id"].nunique() if "station_id" in df.columns else 0
    avg_duration_min = df["session_duration_min"].mean() if "session_duration_min" in df.columns else 0.0

    # Clean records pass rate
    clean_count = int(df["is_clean"].sum()) if "is_clean" in df.columns else total_sessions
    clean_pct = (clean_count / total_sessions * 100) if total_sessions > 0 else 0.0

    # Data Freshness
    latest_ts: Optional[datetime] = None
    freshness_str = "N/A"
    if "connection_time" in df.columns:
        valid_ts = df["connection_time"].dropna()
        if not valid_ts.empty:
            latest_ts = valid_ts.max()
            if isinstance(latest_ts, pd.Timestamp):
                freshness_str = latest_ts.strftime("%Y-%m-%d %H:%M UTC")

    # Format energy: MWh if > 10,000 kWh, else kWh
    if total_energy_kwh >= 10000:
        energy_display = f"{total_energy_kwh / 1000:,.2f}"
        energy_unit = "MWh"
    else:
        energy_display = f"{total_energy_kwh:,.1f}"
        energy_unit = "kWh"

    # Format duration: hours if > 120 mins
    if avg_duration_min >= 120:
        duration_display = f"{avg_duration_min / 60:.1f}"
        duration_unit = "hours"
    else:
        duration_display = f"{avg_duration_min:.0f}"
        duration_unit = "mins"

    c1, c2, c3, c4, c5, c6 = st.columns(6)

    with c1:
        st.markdown(
            f"""
            <div class="kpi-container">
                <div class="kpi-label">Total Sessions</div>
                <div class="kpi-value">{total_sessions:,}</div>
                <div class="kpi-subtext">Plug-in events</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            f"""
            <div class="kpi-container">
                <div class="kpi-label">Energy Delivered</div>
                <div class="kpi-value" style="color: {COLOR_CYAN};">{energy_display} <span style="font-size: 0.9rem; font-weight: normal; color: {COLOR_SLATE};">{energy_unit}</span></div>
                <div class="kpi-subtext">Cumulative metered</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c3:
        st.markdown(
            f"""
            <div class="kpi-container">
                <div class="kpi-label">Active Fleet</div>
                <div class="kpi-value">{active_stations:,}</div>
                <div class="kpi-subtext">Distinct hardware ports</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c4:
        st.markdown(
            f"""
            <div class="kpi-container">
                <div class="kpi-label">Avg Session Duration</div>
                <div class="kpi-value">{duration_display} <span style="font-size: 0.9rem; font-weight: normal; color: {COLOR_SLATE};">{duration_unit}</span></div>
                <div class="kpi-subtext">Mean connected dwell</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c5:
        color_pass = COLOR_EMERALD if clean_pct >= 90 else COLOR_AMBER
        st.markdown(
            f"""
            <div class="kpi-container">
                <div class="kpi-label">Quality Pass Rate</div>
                <div class="kpi-value" style="color: {color_pass};">{clean_pct:.1f}%</div>
                <div class="kpi-subtext">{clean_count:,}/{total_sessions:,} clean records</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c6:
        st.markdown(
            f"""
            <div class="kpi-container">
                <div class="kpi-label">Latest Data Timestamp</div>
                <div class="kpi-value" style="font-size: 1.05rem; padding-top: 0.2rem;">{freshness_str}</div>
                <div class="kpi-subtext">Empirical site coverage</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
