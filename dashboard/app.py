# dashboard/app.py - Version complète et corrigée
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
import requests
from datetime import datetime

# ─── Configuration de la page ────────────────────────────────────────────────
st.set_page_config(
    page_title="Disaster Pipeline Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Styles CSS personnalisés
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

# ─── Connexion à la base de données ──────────────────────────────────────────
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
    """Charge tous les désastres unifiés (GDACS + EONET) depuis la couche Gold"""
    conn = get_connection()
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
    """Charge les statistiques par pays (uniquement GDACS a les données pays)"""
    conn = get_connection()
    return pd.read_sql("""
        SELECT country, iso3, total_disasters, ongoing_count,
               earthquake_count, flood_count, cyclone_count,
               drought_count, volcano_count, wildfire_count,
               total_population_affected, max_alert_level,
               latest_event_date
        FROM disasters_marts.mart_disasters_by_country
        WHERE total_disasters > 0
        ORDER BY total_disasters DESC
    """, conn)

@st.cache_data(ttl=300)
def load_timeline():
    """Charge la timeline des désastres (toutes sources)"""
    conn = get_connection()
    return pd.read_sql("""
        SELECT event_day, event_type, event_type_label,
               disaster_count, population_affected,
               red_alerts, orange_alerts, green_alerts
        FROM disasters_marts.mart_disasters_timeline
        ORDER BY event_day ASC
    """, conn)

# ─── Chargement des données (TOUJOURS depuis la DB unifiée) ──────────────────
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

# ─── Sidebar : Filtres ───────────────────────────────────────────────────────
with st.sidebar:
    st.title("🌍 Disaster Pipeline")
    st.caption("Real-time global disaster monitor")
    st.divider()
    
    st.markdown("### Filters")
    
    # Filtre par type de désastre
    event_types = ["All"] + sorted(df["event_type_label"].dropna().unique().tolist())
    selected_type = st.selectbox("Disaster Type", event_types)
    
    # Filtre par source de données (pour affichage uniquement)
    sources = ["All", "GDACS", "EONET"]
    selected_source = st.selectbox("Data Source", sources)
    
    # Filtre par statut
    statuses = ["All"] + sorted(df["status"].dropna().unique().tolist())
    selected_status = st.selectbox("Status", statuses)
    
    # Filtre par date
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

# ─── Application des filtres ─────────────────────────────────────────────────
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

# ─── En-tête ─────────────────────────────────────────────────────────────────
st.title("🌍 Global Disaster Dashboard")
st.caption("Data from GDACS & NASA EONET · Powered by Airflow + dbt + PostgreSQL")
st.divider()

# ─── KPIs ────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

total = len(filtered)
ongoing = int(filtered["is_active"].sum()) if "is_active" in filtered.columns else 0
gdacs_count = len(filtered[filtered["source_tag"] == "GDACS"]) if "source_tag" in filtered.columns else 0
eonet_count = len(filtered[filtered["source_tag"] == "EONET"]) if "source_tag" in filtered.columns else 0
pop = filtered["population_affected"].fillna(0).sum() if "population_affected" in filtered.columns else 0
countries = filtered["country"].nunique() if "country" in filtered.columns else 0

with k1: st.metric("🌐 Total Events", f"{total:,}")
with k2: st.metric("🔴 Active Now", f"{ongoing:,}")
with k3: st.metric("📡 GDACS Events", f"{gdacs_count:,}")
with k4: st.metric("🛰️ EONET Events", f"{eonet_count:,}")
with k5:
    pop_display = f"{pop/1e6:.1f}M" if pop >= 1e6 else f"{pop/1e3:.0f}K" if pop >= 1e3 else str(int(pop)) if pop > 0 else "N/A"
    st.metric("👥 Pop. Affected", pop_display)

st.divider()

# ─── Carte mondiale (TOUS les événements avec coordonnées) ───────────────────
# ─── Carte mondiale (TOUS les événements avec coordonnées) ───────────────────
st.markdown('<p class="section-header">🗺️ World Map — Disaster Locations</p>', unsafe_allow_html=True)

# Debug: afficher infos sur les données
st.write(f"📊 Total filtered: {len(filtered)}")
if "latitude" in filtered.columns:
    st.write(f"📍 Latitude non-null: {filtered['latitude'].notna().sum()}")
if "longitude" in filtered.columns:
    st.write(f"📍 Longitude non-null: {filtered['longitude'].notna().sum()}")

if not filtered.empty and "latitude" in filtered.columns and "longitude" in filtered.columns:
    map_df = filtered.dropna(subset=["latitude", "longitude"]).copy()
    st.write(f"✅ Events with coords: {len(map_df)}")
    map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
    map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude"])
    
    # Palette de couleurs par type de désastre
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
    
    # Taille des marqueurs selon le niveau d'alerte
    if "alert_level_num" in map_df.columns:
        map_df["marker_size"] = map_df["alert_level_num"].apply(lambda x: 15 if x == 3 else 10 if x == 2 else 7)
    else:
        map_df["marker_size"] = 10
    
    if not map_df.empty:
        fig_map = px.scatter_geo(
            map_df, lat="latitude", lon="longitude",
            color="event_type_label" if "event_type_label" in map_df.columns else None,
            size="marker_size",
            hover_name="event_name" if "event_name" in map_df.columns else None,
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
else:
    st.info("No location data available.")

# ─── Timeline + Diagramme circulaire ─────────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<p class="section-header">📈 Disaster Timeline</p>', unsafe_allow_html=True)
    if not df_timeline.empty and "event_type" in df_timeline.columns and "event_day" in df_timeline.columns:
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
    if not filtered.empty and "event_type_label" in filtered.columns:
        type_counts = filtered["event_type_label"].value_counts().reset_index()
        type_counts.columns = ["type", "count"]
        if not type_counts.empty:
            fig_pie = px.pie(
                type_counts, values="count", names="type",
                color="type", color_discrete_map=color_map if 'color_map' in locals() else None,
                hole=0.55, template="plotly_dark",
            )
            fig_pie.update_layout(
                height=320, paper_bgcolor="#0e1117",
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

# ─── Choropleth + Bar Chart (COMBINÉS - TOUJOURS AFFICHÉS) ───────────────────
st.markdown('<p class="section-header">🌐 Disasters by Country</p>', unsafe_allow_html=True)

# ✅ TOUJOURS afficher la carte choroplèthe si données pays disponibles (GDACS)
if not df_country.empty and "total_disasters" in df_country.columns and df_country["total_disasters"].sum() > 0:
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

# ✅ TOUJOURS afficher le bar chart par type + source (fonctionne pour TOUTES les sources)
if not filtered.empty and "event_type_label" in filtered.columns and "source_tag" in filtered.columns:
    type_source_counts = filtered.groupby(['event_type_label', 'source_tag']).size().reset_index(name='count')
    
    if not type_source_counts.empty:
        fig_fallback = px.bar(
            type_source_counts,
            x='event_type_label',
            y='count',
            color='source_tag',
            barmode='group',
            template='plotly_dark',
            color_discrete_map={'GDACS': '#FF6B6B', 'EONET': '#4ECDC4'}
        )
        fig_fallback.update_layout(
            height=380,
            paper_bgcolor='#0e1117',
            xaxis_title='Disaster Type',
            yaxis_title='Count',
            showlegend=True,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_fallback, use_container_width=True)

# ─── Tableau des événements ──────────────────────────────────────────────────
st.markdown('<p class="section-header">📋 Event Records</p>', unsafe_allow_html=True)
search = st.text_input("🔍 Search", placeholder="e.g. Turkey, Flood, Wildfire...")

if not filtered.empty:
    table_df = filtered.copy()
    if search:
        mask = pd.Series([False] * len(table_df))
        if "event_name" in table_df.columns:
            mask |= table_df["event_name"].str.contains(search, case=False, na=False)
        if "country" in table_df.columns:
            mask |= table_df["country"].str.contains(search, case=False, na=False)
        if "event_type_label" in table_df.columns:
            mask |= table_df["event_type_label"].str.contains(search, case=False, na=False)
        table_df = table_df[mask]

    display_cols = [c for c in ["event_name", "event_type_label", "alert_level", "status",
                    "country", "event_date", "severity_value", "severity_unit",
                    "population_affected", "source_tag", "source_url"] if c in table_df.columns]
    
    if display_cols:
        table_display = table_df[display_cols].copy()
        table_display.columns = ["Name", "Type", "Alert", "Status", "Country",
                                  "Date", "Severity", "Unit", "Pop. Affected", "Source", "URL"][:len(table_display.columns)]
        
        if "Date" in table_display.columns:
            table_display["Date"] = pd.to_datetime(table_display["Date"], errors='coerce').dt.strftime("%Y-%m-%d")
        if "Pop. Affected" in table_display.columns:
            table_display["Pop. Affected"] = table_display["Pop. Affected"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and isinstance(x, (int, float)) and x > 0 else ""
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
else:
    st.info("No records to display.")