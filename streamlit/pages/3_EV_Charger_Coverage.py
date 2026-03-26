import streamlit as st
import pydeck as pdk
from data_loader import load_demand, load_chargers, demand_to_color
from datetime import datetime

st.set_page_config(page_title="EV Charger Coverage", layout="wide")
st.title("⚡ EV Charger Coverage Overlay")
st.caption("Do high-demand zones have adequate charging infrastructure?")

# ── Load ──────────────────────────────────────────────────────
demand_df  = load_demand()
charger_df = load_chargers()

# ── Controls ──────────────────────────────────────────────────
now  = datetime.now()
hour = st.slider("Hour of Day", 0, 23, now.hour, format="%d:00")

filtered = demand_df[demand_df["hour_of_day"] == hour].copy()
filtered["color"] = demand_to_color(filtered["typical_demand"])

# ── Charger stats ─────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Total Charger Stations", len(charger_df))
col2.metric("DCFC (Fast) Stations",
            int((charger_df["fast"] > 0).sum()))
col3.metric("L2 Only Stations",
            int((charger_df["fast"] == 0).sum()))

# ── Layers ────────────────────────────────────────────────────
view_mode = st.radio(
    "Map View",
    ["Demand + Chargers (Flat)", "Demand Only (3D)", "Chargers Only"],
    horizontal=True
)

layers = []

if view_mode in ["Demand + Chargers (Flat)", "Demand Only (3D)"]:
    extruded = view_mode == "Demand Only (3D)"
    opacity  = 0.5 if not extruded else 0.8
    layers.append(pdk.Layer(
        "H3HexagonLayer",
        filtered,
        get_hexagon="pickup_h3",
        get_fill_color="color",
        get_elevation="typical_demand",
        elevation_scale=30,
        extruded=extruded,
        pickable=True,
        opacity=opacity
    ))

if view_mode in ["Demand + Chargers (Flat)", "Chargers Only"]:
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        charger_df,
        get_position=["lon", "lat"],
        get_color="color",
        get_radius=80,
        #radius_min_pixels=5,
        #radius_max_pixels=20,
        stroked=True,
        get_line_color=[255, 255, 255, 200],
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True
    ))

view = pdk.ViewState(
    latitude=40.7128, longitude=-74.0060,
    zoom=10, pitch=40
)

st.pydeck_chart(pdk.Deck(
    layers=layers,
    initial_view_state=view,
    tooltip={
        "html": "<b>{name}</b><br/>Fast: {fast} | L2: {level2}",
        "style": {"color": "white", "backgroundColor": "#333"}
    },
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
))

st.markdown("""
🟢 **Green dot** = DCFC fast charger &nbsp;&nbsp; 🟡 **Yellow dot** = L2 charger only  
*Hex color/height = predicted demand at selected hour*
""")