import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from datetime import datetime

# ─── Config ───────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Disaster Pipeline Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .section-header {
        font-size: 1.1rem; font-weight: 600; color: #e0e0e0;
        border-bottom: 1px solid #2d3250; padding-bottom: 8px; margin-bottom: 16px;
    }
    [data-testid="stMetric"] { background: #1e2130; border-radius: 10px; padding: 12px; }
</style>
""", unsafe_allow_html=True)

# ─── DB Connection ────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "disasters"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline123"),
        port=5432,
    )

@st.cache_data(ttl=300)
def load_disasters():
    conn = get_connection()
    # ← reads from disasters_gold schema (Gold layer)
    return pd.read_sql("""
        SELECT disaster_id, event_type, event_type_label, event_name,
               alert_level, alert_level_num, status, country, iso3,
               latitude, longitude, event_date, event_end_date,
               severity_value, severity_unit, population_affected,
               source_url, source_tag, is_active, event_day, event_month, event_year
        FROM disasters_gold.gold_disasters
        WHERE event_date IS NOT NULL
        ORDER BY event_date DESC
    """, conn)

@st.cache_data(ttl=300)
def load_by_country():
    conn = get_connection()
    return pd.read_sql("""
        SELECT country, iso3, total_disasters, ongoing_count,
               earthquake_count, flood_count, cyclone_count,
               drought_count, volcano_count, wildfire_count,
               total_population_affected, max_alert_level,
               latest_event_date
        FROM disasters_marts.mart_disasters_by_country
        ORDER BY total_disasters DESC
    """, conn)

@st.cache_data(ttl=300)
def load_timeline():
    conn = get_connection()
    return pd.read_sql("""
        SELECT event_day, event_type, event_type_label,
               disaster_count, population_affected,
               red_alerts, orange_alerts, green_alerts
        FROM disasters_marts.mart_disasters_timeline
        ORDER BY event_day ASC
    """, conn)

# ─── Load data ────────────────────────────────────────────────────────────────

try:
    df = load_disasters()
    df_country = load_by_country()
    df_timeline = load_timeline()
    data_ok = True
except Exception as e:
    st.error(f"❌ Database connection failed: {e}")
    st.info("Make sure PostgreSQL is running and dbt models have been executed.")
    data_ok = False
    st.stop()

# ─── Sidebar filters ──────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🌍 Disaster Pipeline")
    st.caption("Real-time global disaster monitor")
    st.divider()
    st.markdown("### Filters")

    event_types = ["All"] + sorted(df["event_type_label"].dropna().unique().tolist())
    selected_type = st.selectbox("Disaster Type", event_types)

    sources = ["All", "GDACS", "EONET"]
    selected_source = st.selectbox("Data Source", sources)

    statuses = ["All"] + sorted(df["status"].dropna().unique().tolist())
    selected_status = st.selectbox("Status", statuses)

    if not df["event_date"].isna().all():
        min_date = pd.to_datetime(df["event_date"]).min().date()
        max_date = pd.to_datetime(df["event_date"]).max().date()
        date_range = st.date_input("Date Range", value=(min_date, max_date),
                                   min_value=min_date, max_value=max_date)
    else:
        date_range = None

    st.divider()
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# ─── Apply filters ────────────────────────────────────────────────────────────

filtered = df.copy()
if selected_type != "All":
    filtered = filtered[filtered["event_type_label"] == selected_type]
if selected_source != "All":
    filtered = filtered[filtered["source_tag"] == selected_source]
if selected_status != "All":
    filtered = filtered[filtered["status"] == selected_status]
if date_range and len(date_range) == 2:
    filtered = filtered[
        (pd.to_datetime(filtered["event_date"]).dt.date >= date_range[0]) &
        (pd.to_datetime(filtered["event_date"]).dt.date <= date_range[1])
    ]

# ─── Header ───────────────────────────────────────────────────────────────────

st.title("🌍 Global Disaster Dashboard")
st.caption("Data from GDACS & NASA EONET · Powered by Airflow + dbt + PostgreSQL")
st.divider()

# ─── KPIs ─────────────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5 = st.columns(5)

total        = len(filtered)
ongoing      = int(filtered["is_active"].sum())
gdacs_count  = len(filtered[filtered["source_tag"] == "GDACS"])
eonet_count  = len(filtered[filtered["source_tag"] == "EONET"])
pop          = filtered["population_affected"].fillna(0).sum()
countries    = filtered["country"].nunique()

with k1: st.metric("🌐 Total Events",   f"{total:,}")
with k2: st.metric("🔴 Active Now",     f"{ongoing:,}")
with k3: st.metric("📡 GDACS Events",   f"{gdacs_count:,}")
with k4: st.metric("🛰️ EONET Events",  f"{eonet_count:,}")
with k5:
    pop_display = f"{pop/1e6:.1f}M" if pop >= 1e6 else f"{pop/1e3:.0f}K" if pop >= 1e3 else str(int(pop)) if pop > 0 else "N/A"
    st.metric("👥 Pop. Affected", pop_display)

st.divider()

# ─── Map ──────────────────────────────────────────────────────────────────────

st.markdown('<p class="section-header">🗺️ World Map — Disaster Locations</p>', unsafe_allow_html=True)

map_df = filtered.dropna(subset=["latitude", "longitude"]).copy()
map_df["latitude"]  = pd.to_numeric(map_df["latitude"],  errors="coerce")
map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
map_df = map_df.dropna(subset=["latitude", "longitude"])

color_map = {
    "Earthquake": "#FF6B6B", "Flood": "#4ECDC4",
    "Tropical Cyclone": "#FFE66D", "Drought": "#F7B731",
    "Volcano": "#FF4757", "Wildfire": "#FF7F50",
    "Tsunami": "#70A1FF", "Other": "#A29BFE",
    "Volcanoes": "#FF4757", "Wildfires": "#FF7F50",
    "Severe Storms": "#FFE66D", "Floods": "#4ECDC4",
    "Earthquakes": "#FF6B6B", "Landslides": "#8B7355",
    "Sea and Lake Ice": "#B0E0E6", "Dust and Haze": "#DEB887",
}

map_df["marker_size"] = map_df["alert_level_num"].apply(lambda x: 15 if x == 3 else 10 if x == 2 else 7)

if not map_df.empty:
    fig_map = px.scatter_geo(
        map_df, lat="latitude", lon="longitude",
        color="event_type_label",
        size="marker_size",
        hover_name="event_name",
        hover_data={"country": True, "alert_level": True, "status": True,
                    "source_tag": True, "event_date": True,
                    "latitude": False, "longitude": False, "marker_size": False},
        color_discrete_map=color_map,
        template="plotly_dark",
        projection="natural earth",
    )
    fig_map.update_layout(
        height=480, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="#0e1117",
        geo=dict(bgcolor="#0e1117", landcolor="#1a1f35", oceancolor="#0d1b2a",
                 showocean=True, showland=True, showcountries=True,
                 countrycolor="#2d3250", showframe=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("No geo-located events match your filters.")

# ─── Timeline + Donut ─────────────────────────────────────────────────────────

col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<p class="section-header">📈 Disaster Timeline</p>', unsafe_allow_html=True)
    tl = df_timeline[df_timeline["event_type"] == "ALL"].copy()
    tl["event_day"] = pd.to_datetime(tl["event_day"])
    if not tl.empty:
        fig_tl = go.Figure()
        fig_tl.add_trace(go.Scatter(
            x=tl["event_day"], y=tl["disaster_count"],
            mode="lines+markers", line=dict(color="#4ECDC4", width=2),
            fill="tozeroy", fillcolor="rgba(78,205,196,0.1)", name="All Events",
        ))
        fig_tl.add_trace(go.Bar(
            x=tl["event_day"], y=tl["red_alerts"],
            marker_color="rgba(255,107,107,0.6)", name="Red Alerts", yaxis="y2",
        ))
        fig_tl.update_layout(
            height=320, template="plotly_dark", paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117", margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(title="Count", gridcolor="#2d3250"),
            yaxis2=dict(title="Red Alerts", overlaying="y", side="right", gridcolor="#2d3250"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis=dict(gridcolor="#2d3250"),
        )
        st.plotly_chart(fig_tl, use_container_width=True)

with col_right:
    st.markdown('<p class="section-header">🍩 Events by Type</p>', unsafe_allow_html=True)
    type_counts = filtered["event_type_label"].value_counts().reset_index()
    type_counts.columns = ["type", "count"]
    if not type_counts.empty:
        fig_pie = px.pie(
            type_counts, values="count", names="type",
            color="type", color_discrete_map=color_map,
            hole=0.55, template="plotly_dark",
        )
        fig_pie.update_layout(
            height=320, paper_bgcolor="#0e1117",
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# ─── Choropleth ───────────────────────────────────────────────────────────────

st.markdown('<p class="section-header">🌐 Disasters by Country</p>', unsafe_allow_html=True)
fig_choro = px.choropleth(
    df_country, locations="iso3", color="total_disasters",
    hover_name="country",
    hover_data={"total_disasters": True, "ongoing_count": True,
                "total_population_affected": True, "iso3": False},
    color_continuous_scale="Reds", template="plotly_dark",
)
fig_choro.update_layout(
    height=380, margin=dict(l=0, r=0, t=0, b=0),
    paper_bgcolor="#0e1117",
    geo=dict(bgcolor="#0e1117", landcolor="#1a1f35", showframe=False),
)
st.plotly_chart(fig_choro, use_container_width=True)

# ─── Table ────────────────────────────────────────────────────────────────────

st.markdown('<p class="section-header">📋 Event Records</p>', unsafe_allow_html=True)
search = st.text_input("🔍 Search", placeholder="e.g. Turkey, Flood, Wildfire...")

table_df = filtered.copy()
if search:
    mask = (
        table_df["event_name"].str.contains(search, case=False, na=False) |
        table_df["country"].str.contains(search, case=False, na=False) |
        table_df["event_type_label"].str.contains(search, case=False, na=False)
    )
    table_df = table_df[mask]

display_cols = ["event_name", "event_type_label", "alert_level", "status",
                "country", "event_date", "severity_value", "severity_unit",
                "population_affected", "source_tag", "source_url"]
table_display = table_df[display_cols].copy()
table_display.columns = ["Name", "Type", "Alert", "Status", "Country",
                          "Date", "Severity", "Unit", "Pop. Affected", "Source", "URL"]
table_display["Date"] = pd.to_datetime(table_display["Date"]).dt.strftime("%Y-%m-%d")
table_display["Pop. Affected"] = table_display["Pop. Affected"].apply(
    lambda x: f"{int(x):,}" if pd.notna(x) and x > 0 else ""
)

st.dataframe(
    table_display,
    use_container_width=True, height=400,
    column_config={
        "Alert": st.column_config.TextColumn(width="small"),
        "Source": st.column_config.TextColumn(width="small"),
        "URL": st.column_config.LinkColumn("Link", display_text="🔗 View"),
    },
)
st.caption(f"Showing {len(table_display):,} of {len(df):,} total records")