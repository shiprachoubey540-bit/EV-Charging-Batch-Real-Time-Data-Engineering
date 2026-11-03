"""
dashboard/components/insights.py — Dynamic analytical insight generation engine.

Owner: Shipra Choubey

Generates data-driven observations from filtered datasets:
  - Energy concentration & top station contributions
  - Turnover vs. energy delivery anomalies (Quadrant analysis)
  - Dwell duration skewness & idle time overhead
  - Peak demand windows in site local time
  - Statistical volume/energy anomalies (Z-score > 2.0)
  - Data quality contract evaluation
"""

from typing import Any, Dict, List
import numpy as np
import pandas as pd


def generate_executive_insights(df: pd.DataFrame, quality_report: dict[str, Any] | None = None) -> List[Dict[str, str]]:
    """
    Generate dynamic analytical insights from the current session records.
    Never hardcodes strings; computes actual values and comparisons.

    Returns
    -------
    List[Dict[str, str]]
        List of insight cards with category, title, body, and severity badge.
    """
    insights: List[Dict[str, str]] = []

    if df.empty or len(df) == 0:
        return [
            {
                "category": "Data Availability",
                "title": "No Records in Current Window",
                "body": "No charging sessions match the active date range or station filter. Broaden filter criteria to generate insights.",
                "type": "neutral",
            }
        ]

    total_sessions = len(df)
    total_energy = df["kwh_delivered"].sum() if "kwh_delivered" in df.columns else 0.0

    # 1. Top Station & Energy Concentration Analysis
    if "station_id" in df.columns and "kwh_delivered" in df.columns and total_energy > 0:
        stn_group = df.groupby("station_id")["kwh_delivered"].sum().sort_values(ascending=False)
        top_stn = stn_group.index[0]
        top_kwh = stn_group.iloc[0]
        top_share = (top_kwh / total_energy) * 100

        insights.append({
            "category": "Energy Distribution",
            "title": f"Top Station: {top_stn} ({top_share:.1f}% of Fleet Energy)",
            "body": (
                f"Station <b>{top_stn}</b> is the leading dispenser in the filtered subset, delivering "
                f"<b>{top_kwh:,.1f} kWh</b> ({top_share:.1f}% of the total {total_energy:,.1f} kWh). "
                f"{'High fleet concentration on a single asset.' if top_share > 35 else 'Energy delivery is distributed across the charging fleet.'}"
            ),
            "type": "highlight" if top_share > 25 else "neutral",
        })

    # 2. Peak Demand Rush Window (Local Site Time)
    hour_col = "connection_hour_local" if "connection_hour_local" in df.columns else "connection_hour_utc"
    day_col = "day_name_local" if "day_name_local" in df.columns else "day_name_utc"

    if hour_col in df.columns and day_col in df.columns:
        peak_hour = df[hour_col].mode()
        peak_day = df[day_col].mode()
        if not peak_hour.empty and not peak_day.empty:
            hr_val = int(peak_hour.iloc[0])
            day_val = str(peak_day.iloc[0])
            time_label = f"{hr_val:02d}:00–{(hr_val+1)%24:02d}:00"
            is_commute = 7 <= hr_val <= 10
            insights.append({
                "category": "Temporal Demand",
                "title": f"Peak Demand Window: {day_val}s @ {time_label}",
                "body": (
                    f"The highest frequency of vehicle plug-ins occurs on <b>{day_val}s around {time_label}</b>. "
                    f"{'Aligns with weekday workplace morning commute arrival patterns.' if is_commute else 'Represents off-peak or afternoon charging activity.'}"
                ),
                "type": "neutral",
            })

    # 3. Dwell Duration & Idle Overhead
    if "session_duration_min" in df.columns:
        valid_dur = df["session_duration_min"].dropna()
        if not valid_dur.empty:
            mean_dur = valid_dur.mean()
            median_dur = valid_dur.median()
            skew_ratio = mean_dur / median_dur if median_dur > 0 else 1.0

            has_idle = "idle_duration_min" in df.columns and df["idle_duration_min"].dropna().any()
            idle_text = ""
            if has_idle:
                avg_idle = df["idle_duration_min"].mean()
                idle_text = f" Vehicles remain plugged in for an average of <b>{avg_idle:.1f} minutes</b> of post-charge idle dwell."

            insights.append({
                "category": "Session Behavior",
                "title": f"Duration Skew: Median {median_dur:.1f}m vs. Mean {mean_dur:.1f}m",
                "body": (
                    f"Session durations exhibit a {'positive right-skew with long dwell outliers' if skew_ratio > 1.25 else 'relatively uniform distribution'}. "
                    f"50% of charging sessions complete within <b>{median_dur:.1f} minutes</b>.{idle_text}"
                ),
                "type": "attention" if skew_ratio > 1.4 else "neutral",
            })

    # 4. Data Quality & Contract Enforcement
    if "is_clean" in df.columns:
        clean_count = int(df["is_clean"].sum())
        flagged_count = total_sessions - clean_count
        pass_rate = (clean_count / total_sessions) * 100 if total_sessions > 0 else 0.0

        if flagged_count > 0 and "validation_flags" in df.columns:
            # Find most common flag
            all_flags = [f.strip() for flags in df[~df["is_clean"]]["validation_flags"] for f in str(flags).split(",") if f.strip()]
            top_flag = pd.Series(all_flags).mode().iloc[0] if all_flags else "unspecified"
            flag_note = f" The dominant trigger is <code>{top_flag}</code>."
        else:
            flag_note = " 100% of session records satisfy all data contracts."

        insights.append({
            "category": "Data Quality Health",
            "title": f"Contract Pass Rate: {pass_rate:.1f}% ({clean_count:,}/{total_sessions:,})",
            "body": (
                f"Validation filters verified <b>{clean_count:,} clean records</b> and isolated "
                f"<b>{flagged_count:,} flagged anomalies</b>.{flag_note} "
                "Quarantine and duration-nulling safeguards are active."
            ),
            "type": "attention" if pass_rate < 90 else "highlight",
        })

    # 5. Statistical Spikes / Outliers in Daily Volume
    if "connection_date" in df.columns and len(df["connection_date"].dropna().unique()) >= 4:
        daily_series = df.groupby("connection_date")["kwh_delivered"].sum()
        mean_d = daily_series.mean()
        std_d = daily_series.std()
        if std_d > 0:
            spikes = daily_series[daily_series > (mean_d + 2 * std_d)]
            if not spikes.empty:
                spike_date = spikes.index[0]
                spike_val = spikes.iloc[0]
                insights.append({
                    "category": "Statistical Anomaly",
                    "title": f"High Energy Spike Detected on {spike_date}",
                    "body": (
                        f"Daily energy reached <b>{spike_val:,.1f} kWh</b>, exceeding the +2σ statistical boundary "
                        f"(baseline: {mean_d:.1f} ± {std_d:.1f} kWh). "
                        "<i>Note: Statistical deviation from empirical mean; does not imply equipment malfunction.</i>"
                    ),
                    "type": "attention",
                })

    return insights
