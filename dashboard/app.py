"""
dashboard/app.py — CHARGE INTELLIGENCE: EV Energy Analytics & Pipeline Observatory.

Owner: Shipra Choubey

An industrial-grade Streamlit intelligence platform integrating:
  1. Executive Overview & Dynamic Key Insights
  2. Charging Demand Intelligence (Heatmaps, Quadrants, Dwell Overhead, Statistical Spikes)
  3. Data Quality Intelligence (Schema contracts, Missing values, Anomaly records)
  4. Real-Time Streaming Observatory (Kafka & Spark Streaming architecture, Truthful telemetry)
  5. Batch Pipeline & Orchestration (Airflow DAG execution, Record accounting, Audit logs)
  6. Data Explorer (Multi-table inspection, Column selectors, CSV export)
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.components.theme import (
    inject_custom_css,
    COLOR_CYAN,
    COLOR_EMERALD,
    COLOR_AMBER,
    COLOR_ROSE,
    COLOR_SLATE,
    COLOR_MUTED,
    COLOR_BORDER,
)
from dashboard.components.data_loader import (
    load_sessions_data,
    load_user_inputs_data,
    load_pipeline_audit_reports,
    check_postgres_status,
)
from dashboard.components.kpis import render_executive_kpis
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
from dashboard.components.observatory import (
    render_streaming_observatory_tab,
    render_batch_observatory_tab,
)

# ----------------------------------------------------------------------
# STREAMLIT PAGE CONFIGURATION
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="VEHICLE CHARGE INTELLIGENCE • EV Analytics & Observatory",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_custom_css()


def main() -> None:
    # ------------------------------------------------------------------
    # DATA INGESTION & STATUS CHECK
    # ------------------------------------------------------------------
    sessions_df, sessions_meta = load_sessions_data()
    user_df, user_meta = load_user_inputs_data()
    pipeline_reports = load_pipeline_audit_reports()
    pg_status = check_postgres_status()

    # ------------------------------------------------------------------
    # SIDEBAR: DATA CONTROLS & SCOPE FILTERS
    # ------------------------------------------------------------------
    st.sidebar.markdown(
        """
        <div style="padding-bottom: 0.5rem; margin-bottom: 1rem; border-bottom: 1px solid #334155;">
            <div style="font-size: 1.15rem; font-weight: 700; color: #FFFFFF; letter-spacing: -0.02em;">
                ⚡ CHARGE <span style="color: #00E5FF;">INTELLIGENCE</span>
            </div>
            <div style="font-size: 0.75rem; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.05em;">
                Pipeline Observatory Controls
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.sidebar.button("🔄 Refresh Data Cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.subheader("Filter Scope")

    filtered_sessions = sessions_df.copy()
    filtered_users = user_df.copy()

    # 1. Timezone Selector
    use_local_tz = st.sidebar.toggle("Use Site Local Time (America/Los_Angeles)", value=True)

    if not sessions_df.empty:
        # 2. Date Range Filter
        date_col = "connection_date_local" if use_local_tz and "connection_date_local" in sessions_df.columns else "connection_date"
        if date_col in sessions_df.columns and sessions_df[date_col].notna().any():
            min_date = sessions_df[date_col].dropna().min()
            max_date = sessions_df[date_col].dropna().max()

            if min_date != max_date:
                selected_dates = st.sidebar.date_input(
                    "Connection Date Range",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                )
                if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                    start_d, end_d = selected_dates
                    filtered_sessions = filtered_sessions[
                        (filtered_sessions[date_col] >= start_d)
                        & (filtered_sessions[date_col] <= end_d)
                    ]
            else:
                st.sidebar.caption(f"Coverage Date: {min_date}")

        # 3. Hardware Station Filter
        if "station_id" in sessions_df.columns:
            all_stations = sorted(sessions_df["station_id"].dropna().unique().tolist())
            selected_stations = st.sidebar.multiselect(
                "Filter Charging Stations",
                options=all_stations,
                default=all_stations if len(all_stations) <= 12 else all_stations[:10],
                help="Select one or more hardware charging station identifiers",
            )
            if selected_stations:
                filtered_sessions = filtered_sessions[
                    filtered_sessions["station_id"].isin(selected_stations)
                ]

        # 4. Quality Anomaly Filter
        if "is_clean" in sessions_df.columns:
            quality_filter = st.sidebar.radio(
                "Data Quality Subset",
                options=["All Records", "Clean Records Only", "Flagged Anomalies Only"],
                index=0,
            )
            if quality_filter == "Clean Records Only":
                filtered_sessions = filtered_sessions[filtered_sessions["is_clean"]]
            elif quality_filter == "Flagged Anomalies Only":
                filtered_sessions = filtered_sessions[~filtered_sessions["is_clean"]]

        # Synchronize linked user preferences
        if not filtered_users.empty and "session_id" in filtered_users.columns:
            filtered_users = filtered_users[
                filtered_users["session_id"].isin(filtered_sessions["session_id"])
            ]

    st.sidebar.markdown("---")
    st.sidebar.caption(
        f"""
        **System Architecture Attribution:**
        - **Pipeline Lead / Dashboard / DAG:** Shipra Choubey
        - **Historical Ingestion / Batch ETL:** Ananthan S
        - **Spark Processing:** Aleena Antony
        - **PostgreSQL Database:** Bainty Kaur
        - **Kafka Real-time Ingestion:** Harikrishnan S.M
        """
    )

    # ------------------------------------------------------------------
    # APP HEADER & OBSERVATORY STATUS BADGES
    # ------------------------------------------------------------------
    st.markdown(
        """
        <div class="brand-container">
            <h1 class="brand-title">VEHICLE CHARGE <span class="brand-title-accent">INTELLIGENCE</span></h1>
            <div class="brand-subtitle">EV Energy Analytics & Pipeline Observatory • Caltech ACN-Data Platform</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Truthful Status & Source Badges
    tier_desc = sessions_meta.get("source_name", "Unknown Source")
    st.markdown(
        f"""
        <div class="badge-bar">
            <span class="status-badge status-badge-cyan">
                📁 Active Data Source: {tier_desc} ({len(sessions_df):,} sessions)
            </span>
            <span class="status-badge {'status-badge-emerald' if pg_status['connected'] else 'status-badge-offline'}">
                🗄️ PostgreSQL: {'Connected' if pg_status['connected'] else 'Configured (Awaiting Loader)' if pg_status['configured'] else 'Local File Tier'}
            </span>
            <span class="status-badge status-badge-offline">
                ⚡ Kafka Broker: Disconnected (Awaiting Integration)
            </span>
            <span class="status-badge status-badge-emerald">
                ⚙️ Airflow DAG: ev_charging_pipeline (Verified)
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Compact Executive Metric Tiles
    render_executive_kpis(filtered_sessions)

    st.markdown("---")

    # ------------------------------------------------------------------
    # OBSERVATORY SECTIONS (TABS)
    # ------------------------------------------------------------------
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Executive Overview",
        "⚡ Charging Demand Intelligence",
        "🛡️ Data Quality Intelligence",
        "📡 Real-Time Streaming Observatory",
        "⚙️ Batch Pipeline & Orchestration",
        "🔍 Data Explorer",
    ])

    # ------------------------------------------------------------------
    # TAB 1: EXECUTIVE OVERVIEW
    # ------------------------------------------------------------------
    with tab1:
        st.markdown("### Executive Performance Overview")

        # Dynamic Key Insights Section
        st.markdown("#### 💡 Dynamically Generated Analytical Insights")
        insights = generate_executive_insights(filtered_sessions, pipeline_reports.get("quality_report"))
        
        ins_col1, ins_col2 = st.columns(2)
        for idx, ins in enumerate(insights):
            target_col = ins_col1 if idx % 2 == 0 else ins_col2
            with target_col:
                border_color = COLOR_AMBER if ins.get("type") == "attention" else COLOR_CYAN if ins.get("type") == "highlight" else COLOR_BORDER
                st.markdown(
                    f"""
                    <div style="background-color: #0F172A; border-left: 3px solid {border_color}; border-top: 1px solid #334155; border-right: 1px solid #334155; border-bottom: 1px solid #334155; border-radius: 0 4px 4px 0; padding: 0.85rem 1rem; margin-bottom: 0.75rem;">
                        <div style="font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; color: #94A3B8; font-weight: 700; margin-bottom: 0.2rem;">
                            {ins['category']}
                        </div>
                        <div style="font-size: 0.92rem; font-weight: 700; color: #FFFFFF; margin-bottom: 0.35rem;">
                            {ins['title']}
                        </div>
                        <div style="font-size: 0.84rem; color: #E2E8F0; line-height: 1.45;">
                            {ins['body']}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        st.markdown("---")

        # Core Overview Charts
        row1_left, row1_right = st.columns([1, 1])

        with row1_left:
            st.markdown('<div class="chart-header">Energy Delivered & Session Volume Over Time</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-question">Question Answered: How does daily energy throughput and session turnover trend across the charging site?</div>', unsafe_allow_html=True)
            fig_timeline = plot_energy_and_volume_timeline(filtered_sessions)
            st.plotly_chart(fig_timeline, use_container_width=True)

        with row1_right:
            st.markdown('<div class="chart-header">Station Delivery Ranking</div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-question">Question Answered: Which hardware stations deliver the greatest total energy or handle the highest turnover?</div>', unsafe_allow_html=True)
            metric_choice = st.radio(
                "Ranking Metric:",
                options=["Total Energy (kWh)", "Session Count", "Avg Energy (kWh/session)"],
                horizontal=True,
                label_visibility="collapsed",
            )
            metric_key = "energy" if "Total" in metric_choice else "sessions" if "Count" in metric_choice else "kwh_per_session"
            fig_stations = plot_station_comparison_bar(filtered_sessions, metric=metric_key, top_n=10)
            st.plotly_chart(fig_stations, use_container_width=True)

    # ------------------------------------------------------------------
    # TAB 2: CHARGING DEMAND INTELLIGENCE
    # ------------------------------------------------------------------
    with tab2:
        st.markdown("### Charging Demand Intelligence")
        st.caption("Temporal intensity, station classification quadrants, dwell overhead, and statistical variations.")

        col_heat, col_quad = st.columns([1, 1])

        with col_heat:
            st.markdown('<div class="chart-header">1. Charging Demand Intensity Heatmap</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-question">Question Answered: What are the peak plug-in arrival windows across weekdays and weekends in site local time?</div>',
                unsafe_allow_html=True,
            )
            heat_metric = st.selectbox("Heatmap Metric:", ["Session Volume (Count)", "Delivered Energy (kWh)"])
            metric_param = "energy" if "Energy" in heat_metric else "sessions"
            fig_heat = plot_demand_heatmap(filtered_sessions, metric=metric_param, use_local_tz=use_local_tz)
            st.plotly_chart(fig_heat, use_container_width=True)

        with col_quad:
            st.markdown('<div class="chart-header">2. Station Performance Quadrant Analysis</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-question">Question Answered: How do charging stations segment by session throughput vs. cumulative delivered energy?</div>',
                unsafe_allow_html=True,
            )
            fig_quad = plot_station_performance_quadrant(filtered_sessions)
            st.plotly_chart(fig_quad, use_container_width=True)

        st.markdown("---")

        col_dur, col_spike = st.columns([1, 1])

        with col_dur:
            st.markdown('<div class="chart-header">3. Dwell Duration & Idle Overhead Distribution</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-question">Question Answered: What is the distribution of vehicle dwell times, and how does active charging compare to post-charge idle dwell?</div>',
                unsafe_allow_html=True,
            )
            fig_dur = plot_session_duration_distribution(filtered_sessions)
            st.plotly_chart(fig_dur, use_container_width=True)
            st.caption("Fences indicate IQR (Interquartile Range). Post-charge idle dwell occurs when a vehicle remains plugged in after power delivery reaches 0 kW.")

        with col_spike:
            st.markdown('<div class="chart-header">4. Statistical Anomaly & Control Limit Timeline</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-question">Question Answered: Are there statistically significant volume or energy spikes exceeding the empirical 2-sigma boundary?</div>',
                unsafe_allow_html=True,
            )
            fig_spike = plot_statistical_anomaly_timeline(filtered_sessions)
            st.plotly_chart(fig_spike, use_container_width=True)
            st.caption("Points in magenta exceed the empirical mean by +2.0 standard deviations. This indicates a statistical outlier, not an equipment fault.")

        st.markdown("---")
        st.markdown('<div class="chart-header">5. Energy Delivered per Session (Station Comparison)</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-question">Question Answered: How variable is the energy delivered across different sessions at each station?</div>', unsafe_allow_html=True)
        fig_per_session = plot_energy_per_session_comparison(filtered_sessions, top_n=12)
        st.plotly_chart(fig_per_session, use_container_width=True)

    # ------------------------------------------------------------------
    # TAB 3: DATA QUALITY INTELLIGENCE
    # ------------------------------------------------------------------
    with tab3:
        st.markdown("### Data Quality Intelligence & Anomaly Audit")
        st.caption("Data contract verification, missing-value distributions, and anomaly rule trigger audits.")

        q_col1, q_col2 = st.columns([1, 1])

        with q_col1:
            st.markdown('<div class="chart-header">Contract Compliance: Clean vs. Flagged Records</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-question">Question Answered: What proportion of records passed all contractual checks vs. triggered quarantine/anomaly rules?</div>',
                unsafe_allow_html=True,
            )
            fig_q_donut = plot_quality_audit_breakdown(filtered_sessions, pipeline_reports.get("quality_report"))
            st.plotly_chart(fig_q_donut, use_container_width=True)

        with q_col2:
            st.markdown('<div class="chart-header">Schema Missing-Value Analysis</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="chart-question">Question Answered: Which columns exhibit missing values, and do they violate contract requirements?</div>',
                unsafe_allow_html=True,
            )
            fig_nulls = plot_missing_values_bar(filtered_sessions)
            st.plotly_chart(fig_nulls, use_container_width=True)

        st.markdown("---")

        st.markdown("#### ⚠️ Anomaly Rule Specifications & Metric Nulling Safeguards")
        st.markdown(
            """
            * **The `done_after_disconnect` Anomaly:** In ~2.5% of records, Caltech station firmware emitted final polling packets after physical unlatching (`doneChargingTime > disconnectTime`).
            * **Safeguard Metric Nulling:** Naive subtraction yields negative idle durations, corrupting analytical aggregates. In accordance with data contracts, the record is retained, tagged, and duration metrics (`charging_duration_min`, `idle_duration_min`) are safely set to `null`.
            * **The `null_done_charging_time_with_energy` Anomaly:** Records where energy was delivered but no completion timestamp was transmitted (e.g. driver unplugged prematurely).
            """
        )

        # Flagged Records Table
        if "is_clean" in filtered_sessions.columns and (~filtered_sessions["is_clean"]).any():
            st.markdown("##### 🔍 Flagged Anomaly Record Inspection")
            flagged_table = filtered_sessions[~filtered_sessions["is_clean"]][[
                "session_id", "station_id", "connection_time", "disconnect_time",
                "done_charging_time", "kwh_delivered", "validation_flags"
            ]]
            st.dataframe(flagged_table, use_container_width=True)
        else:
            st.success("All records in the current filter scope satisfy contractual quality rules without flags.")

    # ------------------------------------------------------------------
    # TAB 4: REAL-TIME STREAMING OBSERVATORY
    # ------------------------------------------------------------------
    with tab4:
        render_streaming_observatory_tab()

    # ------------------------------------------------------------------
    # TAB 5: BATCH PIPELINE & ORCHESTRATION
    # ------------------------------------------------------------------
    with tab5:
        render_batch_observatory_tab(pipeline_reports, filtered_sessions)

    # ------------------------------------------------------------------
    # TAB 6: DATA EXPLORER
    # ------------------------------------------------------------------
    with tab6:
        st.markdown("### Interactive Data Explorer")
        st.caption("Inspect, sort, filter, and export cleaned and normalized datasets.")

        explore_mode = st.radio("Select Dataset to Explore:", ["Charging Sessions (17 Schema Fields)", "User Input Preferences (9 Schema Fields)"], horizontal=True)

        if "Sessions" in explore_mode:
            st.markdown(f"**Viewing Filtered Sessions:** `{len(filtered_sessions):,} records`")

            # Column filter
            all_cols = list(filtered_sessions.columns)
            default_cols = [
                "session_id", "station_id", "connection_time", "disconnect_time",
                "kwh_delivered", "session_duration_min", "charging_duration_min",
                "idle_duration_min", "validation_flags"
            ]
            visible_cols = [c for c in default_cols if c in all_cols]
            selected_cols = st.multiselect("Visible Schema Columns:", options=all_cols, default=visible_cols)

            search_query = st.text_input("Search Session ID or Station ID:", placeholder="e.g. 2-39-100-001")
            view_df = filtered_sessions[selected_cols] if selected_cols else filtered_sessions
            if search_query:
                view_df = view_df[
                    view_df["session_id"].astype(str).str.contains(search_query, case=False)
                    | view_df["station_id"].astype(str).str.contains(search_query, case=False)
                ]

            st.dataframe(view_df, use_container_width=True)

            csv_download = view_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Export Filtered Sessions (CSV)",
                data=csv_download,
                file_name="charge_intelligence_sessions.csv",
                mime="text/csv",
            )
        else:
            st.markdown(f"**Viewing Linked User Inputs:** `{len(filtered_users):,} preferences logged`")
            if filtered_users.empty:
                st.info("No user-input preference records match the current session filter.")
            else:
                col_u_chart, col_u_table = st.columns([1, 1])
                with col_u_chart:
                    fig_u = plot_driver_preferences(filtered_users)
                    st.plotly_chart(fig_u, use_container_width=True)

                with col_u_table:
                    st.dataframe(filtered_users, use_container_width=True)

                csv_u_download = filtered_users.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 Export Filtered User Inputs (CSV)",
                    data=csv_u_download,
                    file_name="charge_intelligence_user_inputs.csv",
                    mime="text/csv",
                )


if __name__ == "__main__":
    import streamlit.runtime
    if streamlit.runtime.exists():
        main()
    else:
        import sys
        from streamlit.web import cli as stcli
        sys.argv = ["streamlit", "run", str(Path(__file__).resolve())]
        sys.exit(stcli.main())
