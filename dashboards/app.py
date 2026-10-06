"""
Wistia Video Analytics dashboards (Streamlit).

Reads the gold layer from dashboards/data/ (populate with sync_gold.py).
No personal data is shown: dim_visitor is used only at country, platform, and browser grain.

    pip install streamlit plotly pandas pyarrow
    streamlit run dashboards/app.py
"""
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DATA = Path(__file__).resolve().parent / "data"

SERIES = {"8hunphufxp": "#2a78d6", "9k4tbcdfg0": "#eb6834"}   # fixed order, never cycled
SEQ_BLUE = ["#d6e6f8", "#9fc3ee", "#5f9be2", "#2a78d6", "#1b4f8f"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"

st.set_page_config(page_title="Wistia Video Analytics", layout="wide")


@st.cache_data
def load(table: str) -> pd.DataFrame:
    df = pd.read_parquet(DATA / table)
    if "media_id" in df.columns:
        df["media_id"] = df["media_id"].astype(str)
    for c in ("date", "snapshot_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c])
    return df


def base_layout(fig: go.Figure, title: str, y_title: str = "", height: int = 380) -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(size=16, color=INK)),
        height=height, margin=dict(l=48, r=24, t=56, b=40),
        plot_bgcolor="#fcfcfb", paper_bgcolor="#fcfcfb",
        font=dict(color=INK2, size=12),
        legend=dict(orientation="h", y=-0.18, x=0, font=dict(color=INK)),
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, zeroline=False)
    fig.update_yaxes(title=y_title, gridcolor=GRID, zeroline=False, linecolor="rgba(0,0,0,0)", rangemode="tozero")
    return fig


media = load("dim_media")
daily = load("fact_media_daily")
engagement = load("fact_media_engagement")
visitors = load("dim_visitor")
cumulative = load("fact_media_cumulative")
curve = load("fact_engagement_curve")

title_of = dict(zip(media.media_id, media.title))
duration_of = dict(zip(media.media_id, media.duration_seconds))

st.sidebar.title("Wistia Video Analytics")
page = st.sidebar.radio("Page", ["Overview", "Media comparison", "Audience", "Engagement curve", "Data quality"])
st.sidebar.caption("Gold layer as of " + str(cumulative.snapshot_date.max().date()))

min_d, max_d = daily.date.min().date(), daily.date.max().date()
default_start = max(min_d, max_d - pd.Timedelta(days=180).to_pytimedelta())
sel = st.sidebar.date_input("Date range", (default_start, max_d), min_value=min_d, max_value=max_d)
start, end = (sel if isinstance(sel, tuple) and len(sel) == 2 else (default_start, max_d))
d = daily[(daily.date.dt.date >= start) & (daily.date.dt.date <= end)]

# ------------------------------------------------------------------ Overview
if page == "Overview":
    st.title("Overview")
    st.caption(f"Daily statistics, {start} to {end}, both media.")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Loads", f"{int(d.load_count.sum()):,}")
    k2.metric("Plays", f"{int(d.play_count.sum()):,}")
    k3.metric("Play rate", f"{d.play_count.sum() / max(d.load_count.sum(), 1):.1%}")
    k4.metric("Hours watched", f"{d.hours_watched.sum():,.1f}")

    for col, label in (("play_count", "Plays per day"), ("load_count", "Loads per day")):
        fig = go.Figure()
        for mid, color in SERIES.items():
            s = d[d.media_id == mid].sort_values("date")
            roll = s[col].rolling(7, min_periods=1).mean()
            fig.add_trace(go.Scatter(x=s.date, y=roll, name=title_of[mid], mode="lines",
                                     line=dict(color=color, width=2),
                                     hovertemplate="%{y:.1f}<extra>" + title_of[mid] + "</extra>"))
        st.plotly_chart(base_layout(fig, f"{label} (7-day rolling mean)", label.split(" per")[0]), use_container_width=True)

# ------------------------------------------------------------------ Media comparison
elif page == "Media comparison":
    st.title("Media comparison")
    agg = d.groupby("media_id").agg(loads=("load_count", "sum"), plays=("play_count", "sum"),
                                    hours=("hours_watched", "sum"), visitors=("unique_visitors", "sum")).reset_index()
    agg["play_rate"] = agg.plays / agg.loads
    agg["title"] = agg.media_id.map(title_of)
    agg["duration_min"] = agg.media_id.map(duration_of) / 60

    c1, c2 = st.columns(2)
    for col, (metric, ytitle, fmt) in zip((c1, c2, c1, c2),
                                          (("plays", "Plays", ",.0f"), ("play_rate", "Play rate", ".1%"),
                                           ("hours", "Hours watched", ",.1f"), ("visitors", "Unique visitors", ",.0f"))):
        fig = go.Figure(go.Bar(x=agg.title, y=agg[metric], marker_color=[SERIES[m] for m in agg.media_id],
                               text=[format(v, fmt) for v in agg[metric]], textposition="outside",
                               textfont=dict(color=INK), hovertemplate="%{y:" + fmt + "}<extra></extra>"))
        fig.update_layout(showlegend=False, bargap=0.45)
        if metric == "play_rate":
            fig.update_yaxes(tickformat=".0%")
        col.plotly_chart(base_layout(fig, ytitle, "", 320), use_container_width=True)

    st.subheader("Cumulative totals by snapshot date")
    fig = go.Figure()
    for mid, color in SERIES.items():
        s = cumulative[cumulative.media_id == mid].sort_values("snapshot_date")
        fig.add_trace(go.Scatter(x=s.snapshot_date, y=s.play_count, name=title_of[mid], mode="lines+markers",
                                 line=dict(color=color, width=2), marker=dict(size=8)))
    st.plotly_chart(base_layout(fig, "Cumulative plays reported by the Stats API, one snapshot per pipeline run", "Plays", 320), use_container_width=True)
    st.dataframe(agg[["title", "duration_min", "loads", "plays", "play_rate", "hours", "visitors"]]
                 .rename(columns={"title": "Media", "duration_min": "Duration (min)", "loads": "Loads", "plays": "Plays",
                                  "play_rate": "Play rate", "hours": "Hours watched", "visitors": "Unique visitors"})
                 .style.format({"Duration (min)": "{:.1f}", "Play rate": "{:.1%}", "Hours watched": "{:,.1f}"}), hide_index=True)

# ------------------------------------------------------------------ Audience
elif page == "Audience":
    st.title("Audience")
    st.caption("Visitors with at least one play on a tracked media. Shown at country, platform, and browser grain only.")

    k1, k2, k3 = st.columns(3)
    k1.metric("Visitors", f"{len(visitors):,}")
    k2.metric("Countries", f"{visitors.country.nunique():,}")
    k3.metric("Mobile share", f"{visitors.is_mobile.mean():.0%}")

    c1, c2 = st.columns(2)
    top = visitors.country.fillna("Unknown").value_counts().head(10).sort_values()
    fig = go.Figure(go.Bar(x=top.values, y=top.index, orientation="h", marker_color=SEQ_BLUE[3],
                           text=top.values, textposition="outside", textfont=dict(color=INK),
                           hovertemplate="%{x:,}<extra></extra>"))
    fig.update_layout(showlegend=False, bargap=0.35)
    fig.update_xaxes(range=[0, top.max() * 1.18])
    c1.plotly_chart(base_layout(fig, "Visitors by country (top 10)", "", 380), use_container_width=True)

    plat = visitors.platform.fillna("Unknown").value_counts().head(8).sort_values()
    fig = go.Figure(go.Bar(x=plat.values, y=plat.index, orientation="h", marker_color=SEQ_BLUE[3],
                           text=plat.values, textposition="outside", textfont=dict(color=INK),
                           hovertemplate="%{x:,}<extra></extra>"))
    fig.update_layout(showlegend=False, bargap=0.35)
    fig.update_xaxes(range=[0, plat.max() * 1.18])
    c2.plotly_chart(base_layout(fig, "Visitors by platform", "", 380), use_container_width=True)

    st.subheader("Watch depth per visitor-day")
    fig = go.Figure()
    bins = list(range(0, 101, 10))
    for mid, color in SERIES.items():
        e = engagement[engagement.media_id == mid]
        h = pd.cut(e.max_watched_percent.clip(0, 100), bins=bins, include_lowest=True).value_counts().sort_index()
        fig.add_trace(go.Bar(x=[f"{b}-{b + 10}" for b in bins[:-1]], y=h.values, name=title_of[mid], marker_color=color,
                             hovertemplate="%{y:,}<extra>" + title_of[mid] + "</extra>"))
    fig.update_layout(barmode="group", bargap=0.3)
    st.plotly_chart(base_layout(fig, "Maximum percent of the video watched, by visitor-day", "Visitor-days", 360), use_container_width=True)

# ------------------------------------------------------------------ Engagement curve
elif page == "Engagement curve":
    st.title("Engagement curve")
    st.caption("Wistia's engagement graph: how many viewers were still watching at each point of the video (latest snapshot).")
    latest = curve.snapshot_date.max()
    cur = curve[curve.snapshot_date == latest]
    fig = go.Figure()
    for mid, color in SERIES.items():
        s = cur[cur.media_id == mid].sort_values("position")
        n = len(s)
        x = s.position / max(n - 1, 1) * 100
        fig.add_trace(go.Scatter(x=x, y=s.engagement, name=title_of[mid], mode="lines", line=dict(color=color, width=2),
                                 hovertemplate="%{y:,.0f} viewers<extra>" + title_of[mid] + "</extra>"))
    fig.update_xaxes(title="Position in video (%)", ticksuffix="%")
    st.plotly_chart(base_layout(fig, f"Viewers still watching, by position (snapshot {latest.date()})", "Viewers", 400), use_container_width=True)

    c1, c2 = st.columns(2)
    for col, mid in zip((c1, c2), SERIES):
        s = cur[cur.media_id == mid].sort_values("position")
        col.metric(title_of[mid], f"{s.overall_engagement.iloc[0]:.1%} overall engagement",
                   help="Share of the video watched, averaged over all plays, as reported by Wistia.")

# ------------------------------------------------------------------ Data quality
else:
    st.title("Data quality")
    st.caption("Reconciliation of the daily series against the cumulative counters published by the Stats API.")
    latest = cumulative[cumulative.snapshot_date == cumulative.snapshot_date.max()].set_index("media_id")
    rows = []
    for mid in SERIES:
        dd = daily[daily.media_id == mid]
        rows.append({"Media": title_of[mid],
                     "Daily loads (sum)": int(dd.load_count.sum()), "Cumulative loads": int(latest.loc[mid, "load_count"]),
                     "Daily plays (sum)": int(dd.play_count.sum()), "Cumulative plays": int(latest.loc[mid, "play_count"]),
                     "Event plays": int(engagement[engagement.media_id == mid].play_count.sum())})
    df = pd.DataFrame(rows)
    df["Play gap"] = (df["Daily plays (sum)"] - df["Cumulative plays"]) / df["Cumulative plays"]
    st.dataframe(df.style.format({"Play gap": "{:+.1%}", **{c: "{:,}" for c in df.columns if "(sum)" in c or "Cumulative" in c or c == "Event plays"}}), hide_index=True)
    st.markdown(
        "- Daily loads and plays are published by `by_date` and summed here; cumulative counters come from `stats/medias/{id}`. "
        "A small shortfall is expected because by_date for the earliest days is sparse.\n"
        "- Event plays count only viewing sessions retained by Wistia (two years) and attributed to a visitor key.\n"
        "- Full findings: `docs/data_quality_inventory.md` in the repository.")
