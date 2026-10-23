"""
dashboard/components/charts.py — Insight-driven analytical visualizations for EV telemetry.

Owner: Shipra Choubey

Visualizations:
  1. Energy & Session Volume Timeline (Dual-Axis with Rolling Trend)
  2. Station Performance Comparison (Ranked Bar Chart)
  3. Charging Demand Heatmap (Day of Week vs. Hour of Day in Site Local Time)
  4. Station Performance Quadrant (Scatter Analysis: Turnover vs. Energy Delivery)
  5. Session Duration & Idle Overhead Distribution (Box & Histogram with IQR fences)
  6. Energy Delivered per Session (Station-level Comparison)
  7. Statistical Anomaly & Trend Analysis (Rolling Mean +/- 2-Sigma Boundary)
  8. Data Quality Health Breakdown & Null Value Analysis
"""

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.components.theme import (
    COLOR_CYAN,
    COLOR_CYAN_DIM,
    COLOR_EMERALD,
    COLOR_AMBER,
    COLOR_ROSE,
    COLOR_SLATE,
    COLOR_MUTED,
    COLOR_BORDER,
    apply_plotly_theme,
)


def plot_energy_and_volume_timeline(df: pd.DataFrame) -> go.Figure:
    """
    Question Answered: How does daily energy delivery and session throughput evolve over time?
    """
    if "connection_date" not in df.columns or df["connection_date"].dropna().empty:
        fig = go.Figure()
        return apply_plotly_theme(fig, "Daily Energy & Volume Timeline (No Date Data)")

    daily = (
        df.dropna(subset=["connection_date"])
        .groupby("connection_date")
        .agg(
            daily_kwh=("kwh_delivered", "sum"),
            session_count=("session_id", "count"),
        )
        .reset_index()
        .sort_values("connection_date")
    )

    daily["rolling_kwh_7d"] = daily["daily_kwh"].rolling(window=7, min_periods=1).mean()

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 1. Daily Delivered Energy Bar
    fig.add_trace(
        go.Bar(
            x=daily["connection_date"],
            y=daily["daily_kwh"],
            name="Delivered Energy (kWh)",
            marker_color=COLOR_CYAN_DIM,
            opacity=0.6,
            hovertemplate="<b>%{x}</b><br>Energy: %{y:,.1f} kWh<extra></extra>",
        ),
        secondary_y=False,
    )

    # 2. 7-Day Rolling Trendline
    fig.add_trace(
        go.Scatter(
            x=daily["connection_date"],
            y=daily["rolling_kwh_7d"],
            name="7-Day Rolling Mean (kWh)",
            line=dict(color=COLOR_CYAN, width=2.5),
            hovertemplate="7-Day Avg: %{y:,.1f} kWh<extra></extra>",
        ),
        secondary_y=False,
    )

    # 3. Session Volume Line
    fig.add_trace(
        go.Scatter(
            x=daily["connection_date"],
            y=daily["session_count"],
            name="Session Count",
            line=dict(color=COLOR_AMBER, width=2, dash="dot"),
            mode="lines+markers",
            marker=dict(size=4),
            hovertemplate="Sessions: %{y:,}<extra></extra>",
        ),
        secondary_y=True,
    )

    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Energy Delivered (kWh)", secondary_y=False)
    fig.update_yaxes(title_text="Session Count", secondary_y=True, showgrid=False)
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return apply_plotly_theme(fig, height=360)


def plot_station_comparison_bar(df: pd.DataFrame, metric: str = "energy", top_n: int = 15) -> go.Figure:
    """
    Question Answered: Which hardware stations deliver the most energy or experience highest turnover?
    """
    if "station_id" not in df.columns or "kwh_delivered" not in df.columns:
        return apply_plotly_theme(go.Figure(), "Station Performance")

    station_agg = (
        df.groupby("station_id")
        .agg(
            total_kwh=("kwh_delivered", "sum"),
            session_count=("session_id", "count"),
            avg_duration=("session_duration_min", "mean"),
        )
        .reset_index()
    )

    station_agg["kwh_per_session"] = station_agg["total_kwh"] / station_agg["session_count"]

    if metric == "sessions":
        station_agg = station_agg.sort_values(by="session_count", ascending=True)
        x_col = "session_count"
        x_title = "Total Sessions Processed"
        bar_color = COLOR_AMBER
        text_fmt = "%{x:,}"
    elif metric == "kwh_per_session":
        station_agg = station_agg.sort_values(by="kwh_per_session", ascending=True)
        x_col = "kwh_per_session"
        x_title = "Average Energy per Session (kWh/session)"
        bar_color = COLOR_EMERALD
        text_fmt = "%{x:.1f} kWh"
    else:
        station_agg = station_agg.sort_values(by="total_kwh", ascending=True)
        x_col = "total_kwh"
        x_title = "Total Delivered Energy (kWh)"
        bar_color = COLOR_CYAN
        text_fmt = "%{x:,.1f}"

    if top_n and len(station_agg) > top_n:
        station_agg = station_agg.tail(top_n)

    fig = go.Figure(
        go.Bar(
            x=station_agg[x_col],
            y=station_agg["station_id"],
            orientation="h",
            marker_color=bar_color,
            text=station_agg[x_col],
            texttemplate=text_fmt,
            textposition="auto",
            hovertemplate="<b>Station: %{y}</b><br>" + f"{x_title}: " + "%{x}<extra></extra>",
        )
    )
    fig.update_xaxes(title_text=x_title)
    fig.update_yaxes(title_text="Hardware Station ID")
    return apply_plotly_theme(fig, height=max(320, len(station_agg) * 24))


def plot_demand_heatmap(df: pd.DataFrame, metric: str = "sessions", use_local_tz: bool = True) -> go.Figure:
    """
    Question Answered: What are the peak charging demand windows by day of week and hour?
    """
    hour_col = "connection_hour_local" if use_local_tz and "connection_hour_local" in df.columns else "connection_hour_utc"
    day_col = "day_name_local" if use_local_tz and "day_name_local" in df.columns else "day_name_utc"

    if hour_col not in df.columns or day_col not in df.columns:
        return apply_plotly_theme(go.Figure(), "Charging Demand Heatmap")

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    if metric == "energy" and "kwh_delivered" in df.columns:
        pivot = (
            df.groupby([day_col, hour_col])["kwh_delivered"]
            .sum()
            .unstack(fill_value=0.0)
            .reindex(day_order)
            .fillna(0.0)
        )
        val_label = "Delivered Energy (kWh)"
        colorscale = "Viridis"
    else:
        pivot = (
            df.groupby([day_col, hour_col])
            .size()
            .unstack(fill_value=0)
            .reindex(day_order)
            .fillna(0)
        )
        val_label = "Sessions"
        colorscale = "Blues"

    for h in range(24):
        if h not in pivot.columns:
            pivot[h] = 0
    pivot = pivot[sorted(pivot.columns)]

    tz_label = "Site Local (America/Los_Angeles)" if use_local_tz else "UTC"

    fig = px.imshow(
        pivot,
        labels=dict(x=f"Hour of Day ({tz_label})", y="Day of Week", color=val_label),
        x=[f"{h:02d}:00" for h in pivot.columns],
        y=pivot.index,
        color_continuous_scale=colorscale,
        aspect="auto",
    )
    fig.update_layout(
        coloraxis_colorbar=dict(title=val_label, thickness=14, len=0.8),
    )
    return apply_plotly_theme(fig, height=330)


def plot_station_performance_quadrant(df: pd.DataFrame) -> go.Figure:
    """
    Question Answered: How do stations segment by turnover volume vs total energy delivery?
    """
    if "station_id" not in df.columns or "kwh_delivered" not in df.columns:
        return apply_plotly_theme(go.Figure(), "Station Performance Quadrant")

    stn = (
        df.groupby("station_id")
        .agg(
            session_count=("session_id", "count"),
            total_kwh=("kwh_delivered", "sum"),
            avg_duration=("session_duration_min", "mean"),
        )
        .reset_index()
    )

    stn["kwh_per_session"] = stn["total_kwh"] / stn["session_count"]

    med_x = stn["session_count"].median()
    med_y = stn["total_kwh"].median()

    # Classify Quadrants
    def classify_station(row):
        if row["session_count"] >= med_x and row["total_kwh"] >= med_y:
            return "Core Workhorses (High Vol / High Energy)"
        elif row["session_count"] < med_x and row["total_kwh"] >= med_y:
            return "Heavy Delivery Hubs (Low Vol / High Energy)"
        elif row["session_count"] >= med_x and row["total_kwh"] < med_y:
            return "High Turnover / Short Top-Up (High Vol / Low Energy)"
        else:
            return "Underutilized (Low Vol / Low Energy)"

    stn["Quadrant"] = stn.apply(classify_station, axis=1)

    color_map = {
        "Core Workhorses (High Vol / High Energy)": COLOR_CYAN,
        "Heavy Delivery Hubs (Low Vol / High Energy)": COLOR_EMERALD,
        "High Turnover / Short Top-Up (High Vol / Low Energy)": COLOR_AMBER,
        "Underutilized (Low Vol / Low Energy)": COLOR_MUTED,
    }

    fig = px.scatter(
        stn,
        x="session_count",
        y="total_kwh",
        size="kwh_per_session",
        color="Quadrant",
        color_discrete_map=color_map,
        hover_name="station_id",
        hover_data={
            "session_count": ":,",
            "total_kwh": ":,.1f",
            "kwh_per_session": ":.2f",
            "Quadrant": False,
        },
        labels={
            "session_count": "Total Sessions Processed (Turnover Proxy)",
            "total_kwh": "Total Energy Delivered (kWh)",
            "kwh_per_session": "Avg kWh/Session",
        },
    )

    # Median Reference Lines
    fig.add_vline(x=med_x, line_width=1, line_dash="dash", line_color=COLOR_BORDER)
    fig.add_hline(y=med_y, line_width=1, line_dash="dash", line_color=COLOR_BORDER)

    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return apply_plotly_theme(fig, height=420)


def plot_session_duration_distribution(df: pd.DataFrame) -> go.Figure:
    """
    Question Answered: What is the distribution of vehicle dwell times, and how does active charging compare to idle dwell?
    """
    if "session_duration_min" not in df.columns:
        return apply_plotly_theme(go.Figure(), "Duration Distribution")

    has_idle = "idle_duration_min" in df.columns and df["idle_duration_min"].dropna().any()
    has_charge = "charging_duration_min" in df.columns and df["charging_duration_min"].dropna().any()

    fig = go.Figure()

    # Total Plug-in Duration
    fig.add_trace(
        go.Box(
            y=df["session_duration_min"],
            name="Total Connected Dwell",
            marker_color=COLOR_CYAN,
            boxmean=True,
            hovertemplate="Total Dwell: %{y:.1f} mins<extra></extra>",
        )
    )

    if has_charge:
        valid_charge = df["charging_duration_min"].dropna()
        fig.add_trace(
            go.Box(
                y=valid_charge,
                name="Active Charging Duration",
                marker_color=COLOR_EMERALD,
                boxmean=True,
                hovertemplate="Active Charge: %{y:.1f} mins<extra></extra>",
            )
        )

    if has_idle:
        valid_idle = df["idle_duration_min"].dropna()
        fig.add_trace(
            go.Box(
                y=valid_idle,
                name="Post-Charge Idle Dwell",
                marker_color=COLOR_AMBER,
                boxmean=True,
                hovertemplate="Idle Dwell: %{y:.1f} mins<extra></extra>",
            )
        )

    fig.update_yaxes(title_text="Duration (Minutes)")
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return apply_plotly_theme(fig, height=360)


def plot_energy_per_session_comparison(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    """
    Question Answered: How does delivered energy vary across individual sessions and stations?
    """
    if "station_id" not in df.columns or "kwh_delivered" not in df.columns:
        return apply_plotly_theme(go.Figure(), "Energy per Session Distribution")

    # Filter to top N active stations
    top_stations = df["station_id"].value_counts().head(top_n).index
    subset = df[df["station_id"].isin(top_stations)]

    fig = px.box(
        subset,
        x="station_id",
        y="kwh_delivered",
        color="station_id",
        labels={"station_id": "Hardware Station ID", "kwh_delivered": "Energy per Session (kWh)"},
    )
    fig.update_layout(showlegend=False)
    fig.update_xaxes(tickangle=-45)
    return apply_plotly_theme(fig, height=360)


def plot_statistical_anomaly_timeline(df: pd.DataFrame) -> go.Figure:
    """
    Question Answered: Are there statistically significant spikes or drops in daily energy demand?
    """
    if "connection_date" not in df.columns or df["connection_date"].dropna().empty:
        return apply_plotly_theme(go.Figure(), "Statistical Anomaly Timeline")

    daily = (
        df.dropna(subset=["connection_date"])
        .groupby("connection_date")["kwh_delivered"]
        .sum()
        .reset_index()
        .sort_values("connection_date")
    )

    if len(daily) < 3:
        # Fallback simple bar
        fig = px.bar(daily, x="connection_date", y="kwh_delivered", labels={"connection_date": "Date", "kwh_delivered": "Energy (kWh)"})
        return apply_plotly_theme(fig, height=340)

    mean_kwh = daily["kwh_delivered"].mean()
    std_kwh = daily["kwh_delivered"].std() if daily["kwh_delivered"].std() > 0 else 1.0

    upper_bound = mean_kwh + 2.0 * std_kwh
    lower_bound = max(0.0, mean_kwh - 2.0 * std_kwh)

    daily["is_anomaly"] = (daily["kwh_delivered"] > upper_bound) | (daily["kwh_delivered"] < lower_bound)

    fig = go.Figure()

    # Normal Points
    normal_pts = daily[~daily["is_anomaly"]]
    fig.add_trace(
        go.Scatter(
            x=normal_pts["connection_date"],
            y=normal_pts["kwh_delivered"],
            mode="lines+markers",
            name="Daily Energy",
            line=dict(color=COLOR_CYAN, width=2),
            marker=dict(size=6),
            hovertemplate="<b>%{x}</b>: %{y:,.1f} kWh<extra></extra>",
        )
    )

    # Anomalies
    anomaly_pts = daily[daily["is_anomaly"]]
    if not anomaly_pts.empty:
        fig.add_trace(
            go.Scatter(
                x=anomaly_pts["connection_date"],
                y=anomaly_pts["kwh_delivered"],
                mode="markers",
                name="Statistical Outlier (>2σ)",
                marker=dict(color=COLOR_ROSE, size=10, symbol="diamond"),
                hovertemplate="<b>Outlier Date: %{x}</b><br>Energy: %{y:,.1f} kWh<extra></extra>",
            )
        )

    # Upper/Lower Boundary Lines
    fig.add_hline(y=mean_kwh, line_width=1, line_dash="dash", line_color=COLOR_SLATE, annotation_text=f"Mean: {mean_kwh:.1f}")
    fig.add_hline(y=upper_bound, line_width=1, line_dash="dot", line_color=COLOR_AMBER, annotation_text=f"+2σ ({upper_bound:.1f})")

    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Energy Delivered (kWh)")
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return apply_plotly_theme(fig, height=340)


def plot_quality_audit_breakdown(df: pd.DataFrame, quality_report: dict | None = None) -> go.Figure:
    """
    Question Answered: What proportion of records satisfied data contracts versus triggered anomaly flags?
    """
    if "is_clean" not in df.columns:
        return apply_plotly_theme(go.Figure(), "Quality Breakdown")

    total = len(df)
    clean = int(df["is_clean"].sum())
    flagged = total - clean

    fig = go.Figure(
        go.Pie(
            labels=["Clean Records (Contract Compliant)", "Flagged Anomaly Records"],
            values=[clean, flagged],
            hole=0.6,
            marker_colors=[COLOR_EMERALD, COLOR_AMBER],
            textinfo="percent+value",
            hovertemplate="<b>%{label}</b><br>Records: %{value:,} (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return apply_plotly_theme(fig, height=320)


def plot_missing_values_bar(df: pd.DataFrame) -> go.Figure:
    """
    Question Answered: Which columns contain missing values, and does it breach schema requirements?
    """
    if df.empty:
        return apply_plotly_theme(go.Figure(), "Missing Value Analysis")

    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df)) * 100

    null_df = pd.DataFrame({
        "Column": null_counts.index,
        "Missing_Count": null_counts.values,
        "Missing_Pct": null_pct.values,
    }).sort_values(by="Missing_Pct", ascending=False)

    fig = go.Figure(
        go.Bar(
            x=null_df["Column"],
            y=null_df["Missing_Pct"],
            text=null_df["Missing_Pct"].apply(lambda v: f"{v:.1f}%" if v > 0 else "0%"),
            textposition="auto",
            marker_color=[COLOR_ROSE if v > 20 else COLOR_AMBER if v > 0 else COLOR_EMERALD for v in null_df["Missing_Pct"]],
            hovertemplate="<b>%{x}</b><br>Null Rate: %{y:.2f}%<extra></extra>",
        )
    )
    fig.update_yaxes(title_text="Missing Percentage (%)", range=[0, 105])
    fig.update_xaxes(title_text="Schema Column", tickangle=-45)
    return apply_plotly_theme(fig, height=340)


def plot_driver_preferences(user_df: pd.DataFrame) -> go.Figure:
    """
    Question Answered: How does driver vehicle efficiency correlate with requested charging energy?
    """
    if user_df.empty or "kwh_requested" not in user_df.columns or "wh_per_mile" not in user_df.columns:
        return apply_plotly_theme(go.Figure(), "Driver Preferences Analysis")

    clean = user_df.dropna(subset=["kwh_requested", "wh_per_mile"])
    if clean.empty:
        return apply_plotly_theme(go.Figure(), "Driver Preferences (No Data)")

    fig = px.scatter(
        clean,
        x="wh_per_mile",
        y="kwh_requested",
        color="payment_required" if "payment_required" in clean.columns else None,
        color_discrete_map={True: COLOR_CYAN, False: COLOR_EMERALD},
        size="miles_requested" if "miles_requested" in clean.columns else None,
        hover_data=["session_id"] if "session_id" in clean.columns else None,
        labels={
            "wh_per_mile": "Vehicle Consumption Rating (Wh/mi)",
            "kwh_requested": "Driver Requested Energy (kWh)",
            "payment_required": "Payment Required",
        },
    )
    return apply_plotly_theme(fig, height=360)
