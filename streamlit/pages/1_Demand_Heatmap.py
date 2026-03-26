import streamlit as st
import pydeck as pdk
import time
from data_loader import load_demand, demand_to_color

st.set_page_config(page_title="Demand Heatmap", layout="wide")
st.title("NYC Taxi Demand Heatmap")

# ── Load data ────────────────────────────────────────────────
demand_df = load_demand()

DAY_LABELS = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
              5: "Thursday", 6: "Friday", 7: "Saturday"}

# ── Controls ─────────────────────────────────────────────────
col1, col2 = st.columns([2, 1])
with col1:
    hour = st.slider("Hour of Day", 0, 23, 8, key="hour_slider",
                     format="%d:00")
with col2:
    dow_label = st.selectbox("Day of Week", list(DAY_LABELS.values()), index=1)
    dow = {v: k for k, v in DAY_LABELS.items()}[dow_label]

# ── Filter + color ────────────────────────────────────────────
def get_map_data(hour, dow):
    filtered = demand_df[
        (demand_df["hour_of_day"] == hour) &
        (demand_df["day_of_week"] == dow)
    ].copy()
    filtered["color"] = demand_to_color(filtered["typical_demand"])
    filtered["typical_demand"] = filtered["typical_demand"].round()
    return filtered

map_data = get_map_data(hour, dow)

st.caption(f"Showing {len(map_data)} active H3 cells for {dow_label} at {hour:02d}:00")

# ── PyDeck map ────────────────────────────────────────────────
layer = pdk.Layer(
    "H3HexagonLayer",
    map_data,
    get_hexagon="pickup_h3",
    get_fill_color="color",
    get_elevation="typical_demand",
    elevation_scale=50,
    elevation_range=[0, 1000],
    extruded=True,
    pickable=True,
    auto_highlight=True
)

view = pdk.ViewState(
    latitude=40.7128, longitude=-74.0060,
    zoom=10, pitch=45, bearing=0
)

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view,
    tooltip={"text": "H3: {pickup_h3}\nDemand: {typical_demand}"},
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
))

# ── Legend ────────────────────────────────────────────────────
st.markdown("""
🔵 Low demand &nbsp;&nbsp; 🟡 Medium demand &nbsp;&nbsp; 🔴 High demand  
*Height represents predicted pickup count per 5-minute window*
""")

# ── Time-lapse ────────────────────────────────────────────────
st.markdown("---")
st.subheader("⏱️ 24-Hour Demand Time-Lapse")
st.caption("Watch demand shift across NYC through a full day")

col_a, col_b, col_c = st.columns([1, 1, 2])
play      = col_a.button("▶ Play")
speed     = col_b.selectbox("Speed", ["Slow", "Normal", "Fast"],
                             index=1, label_visibility="collapsed")
delay_map = {"Slow": 1.0, "Normal": 0.5, "Fast": 0.2}
delay     = delay_map[speed]

if play:
    frame_slot = st.empty()
    caption_slot = st.empty()

    for h in range(24):
        frame_data = get_map_data(h, dow)
        frame_layer = pdk.Layer(
            "H3HexagonLayer",
            frame_data,
            get_hexagon="pickup_h3",
            get_fill_color="color",
            get_elevation="typical_demand",
            elevation_scale=50,
            extruded=True,
            pickable=False
        )
        caption_slot.markdown(f"### 🕐 {h:02d}:00 — {dow_label}")
        frame_slot.pydeck_chart(pdk.Deck(
            layers=[frame_layer],
            initial_view_state=view,
            map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
        ))
        time.sleep(delay)

    caption_slot.markdown("### ✅ Time-lapse complete")