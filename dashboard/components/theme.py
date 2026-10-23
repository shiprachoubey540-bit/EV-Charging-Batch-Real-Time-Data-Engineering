"""
dashboard/components/theme.py — Metallic Titanium & Brushed Steel Analytics Theme.

Owner: Shipra Choubey

Visual Direction:
  - Metallic cool platinum / aluminum surfaces (#E2E8F0, #EDF2F7, #F1F5F9)
  - Brushed steel card containers (#FFFFFF, #F8FAFC) with subtle chrome borders (#CBD5E1)
  - High-contrast gunmetal text hierarchy (#0F172A, #334155, #64748B)
  - Metallic cobalt (#0284C7), emerald (#059669), and bronze/amber (#D97706) accents
"""

import plotly.graph_objects as go
import streamlit as st

# Metallic Titanium Palette Constants
COLOR_BG_PRIMARY = "#EDF2F7"       # Metallic cool slate/aluminum canvas
COLOR_BG_SURFACE = "#E2E8F0"       # Brushed titanium surface
COLOR_BG_CARD = "#FFFFFF"          # Polished platinum card container
COLOR_BORDER = "#CBD5E1"           # Brushed chrome/steel border

COLOR_CYAN = "#0284C7"             # Electric Cobalt / Metallic Cyan
COLOR_CYAN_DIM = "#0284C7"         # Cobalt Midtone
COLOR_EMERALD = "#059669"          # Industrial Emerald
COLOR_AMBER = "#D97706"            # Industrial Bronze / Amber
COLOR_ROSE = "#E11D48"             # Crimson Alert
COLOR_SLATE = "#475569"            # Steel Slate
COLOR_TEXT = "#0F172A"             # Gunmetal Navy (Primary High-Contrast Text)
COLOR_MUTED = "#64748B"            # Muted Steel Grey


def inject_custom_css() -> None:
    """Inject custom metallic industrial stylesheet into Streamlit page."""
    st.markdown(
        f"""
        <style>
        /* Base page background: Metallic Brushed Steel Gradient */
        .stApp {{
            background: linear-gradient(145deg, #EEF2F6 0%, #E2E8F0 50%, #D8DFE8 100%);
            color: {COLOR_TEXT};
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        }}

        /* App Header */
        .brand-container {{
            padding: 1.25rem 0 1rem 0;
            border-bottom: 2px solid {COLOR_BORDER};
            margin-bottom: 1.5rem;
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.4) 0%, rgba(241, 245, 249, 0) 100%);
            border-radius: 4px;
        }}
        .brand-title {{
            font-size: 2.1rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: {COLOR_TEXT};
            margin: 0;
            line-height: 1.2;
            display: flex;
            align-items: center;
            gap: 0.6rem;
        }}
        .brand-title-accent {{
            color: {COLOR_CYAN};
        }}
        .brand-subtitle {{
            font-size: 0.95rem;
            color: {COLOR_SLATE};
            font-weight: 600;
            margin-top: 0.25rem;
            letter-spacing: 0.03em;
            text-transform: uppercase;
        }}

        /* Metallic Status & Source Badges */
        .badge-bar {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
            margin-top: 0.75rem;
            margin-bottom: 1.25rem;
        }}
        .status-badge {{
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            padding: 0.3rem 0.75rem;
            border-radius: 5px;
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.03em;
            text-transform: uppercase;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
        }}
        .status-badge-cyan {{
            background-color: #E0F2FE;
            color: #0369A1;
            border: 1px solid #BAE6FD;
        }}
        .status-badge-emerald {{
            background-color: #ECFDF5;
            color: #047857;
            border: 1px solid #A7F3D0;
        }}
        .status-badge-amber {{
            background-color: #FFFBEB;
            color: #B45309;
            border: 1px solid #FDE68A;
        }}
        .status-badge-offline {{
            background-color: #F1F5F9;
            color: {COLOR_SLATE};
            border: 1px solid {COLOR_BORDER};
        }}

        /* Polished Metallic KPI Metric Cards */
        .kpi-container {{
            background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%);
            border: 1px solid {COLOR_BORDER};
            border-radius: 6px;
            padding: 0.85rem 1rem;
            margin-bottom: 0.75rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06), inset 0 1px 0 #FFFFFF;
            transition: all 0.2s ease;
        }}
        .kpi-container:hover {{
            border-color: {COLOR_CYAN};
            box-shadow: 0 4px 6px -1px rgba(2, 132, 199, 0.12), inset 0 1px 0 #FFFFFF;
            transform: translateY(-1px);
        }}
        .kpi-label {{
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: {COLOR_SLATE};
            font-weight: 700;
            margin-bottom: 0.25rem;
        }}
        .kpi-value {{
            font-size: 1.55rem;
            font-weight: 800;
            color: {COLOR_TEXT};
            line-height: 1.1;
            font-variant-numeric: tabular-nums;
        }}
        .kpi-subtext {{
            font-size: 0.75rem;
            color: {COLOR_MUTED};
            margin-top: 0.35rem;
            font-weight: 500;
        }}

        /* Section Container & Insight Cards */
        .analytics-card {{
            background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%);
            border: 1px solid {COLOR_BORDER};
            border-radius: 6px;
            padding: 1.25rem;
            margin-bottom: 1.25rem;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.05);
        }}

        /* Chart Header & Context Banner */
        .chart-header {{
            font-size: 1.05rem;
            font-weight: 700;
            color: {COLOR_TEXT};
            margin-bottom: 0.15rem;
        }}
        .chart-question {{
            font-size: 0.82rem;
            color: {COLOR_SLATE};
            margin-bottom: 0.75rem;
            font-style: italic;
        }}

        /* Streamlit Tab Bar Styling */
        .stTabs [data-baseweb="tab-list"] {{
            background-color: #E2E8F0;
            border-radius: 6px;
            padding: 0.3rem;
            border: 1px solid {COLOR_BORDER};
            gap: 0.25rem;
        }}
        .stTabs [data-baseweb="tab"] {{
            color: {COLOR_SLATE};
            font-weight: 600;
            border-radius: 4px;
            padding: 0.4rem 0.85rem;
        }}
        .stTabs [aria-selected="true"] {{
            background-color: #FFFFFF !important;
            color: {COLOR_CYAN} !important;
            border-radius: 4px;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.1);
        }}

        /* Sidebar Styling: Brushed Titanium Metal */
        section[data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #E2E8F0 0%, #D8DFE8 100%);
            border-right: 1px solid {COLOR_BORDER};
        }}

        /* Text Overrides */
        h1, h2, h3, h4, h5, h6 {{
            color: {COLOR_TEXT} !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_plotly_theme(fig: go.Figure, title: str = "", height: int = 380) -> go.Figure:
    """
    Apply unified metallic industrial theme to Plotly figure with crisp light surfaces and readable labels.
    """
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(255, 255, 255, 0.85)",
        plot_bgcolor="rgba(248, 250, 252, 0.95)",
        font=dict(
            family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
            size=11,
            color=COLOR_TEXT,
        ),
        title=dict(
            text=f"<b>{title}</b>" if title else None,
            font=dict(color=COLOR_TEXT, size=13),
            x=0.01,
            y=0.96,
        ) if title else None,
        margin=dict(l=40, r=25, t=45 if title else 20, b=35),
        height=height,
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            bordercolor=COLOR_BORDER,
            font=dict(color=COLOR_TEXT, size=11),
        ),
    )
    fig.update_xaxes(
        gridcolor="#E2E8F0",
        zerolinecolor=COLOR_BORDER,
        tickfont=dict(color=COLOR_SLATE, size=10),
    )
    fig.update_yaxes(
        gridcolor="#E2E8F0",
        zerolinecolor=COLOR_BORDER,
        tickfont=dict(color=COLOR_SLATE, size=10),
    )
    return fig
