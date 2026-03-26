import streamlit as st
import pydeck as pdk
import requests
import h3
import pandas as pd

st.set_page_config(page_title="Driver Simulator", layout="wide")
st.title("🗺️ Driver Recommendation Simulator")

API_URL = "http://localhost:8000"

BOROUGH_COORDS = {
    "Manhattan (Midtown)":    (40.7580, -73.9855),
    "Manhattan (Downtown)":   (40.7128, -74.0060),
    "Brooklyn (Downtown)":    (40.6928, -73.9903),
    "Queens (Flushing)":      (40.7677, -73.8330),
    "Bronx":                  (40.8501, -73.8662),
    "JFK Airport":            (40.6413, -73.7781),
    "LaGuardia Airport":      (40.7769, -73.8740),
}

# ── Input ─────────────────────────────────────────────────────
st.markdown("#### Select driver starting location")
col1, col2 = st.columns([1, 1])

with col1:
    mode = st.radio("Input mode", ["Borough preset", "Custom coordinates"],
                    horizontal=True)

with col2:
    if mode == "Borough preset":
        borough = st.selectbox("Borough", list(BOROUGH_COORDS.keys()))
        lat, lon = BOROUGH_COORDS[borough]
    else:
        lat = st.number_input("Latitude",  value=40.7580, format="%.4f")
        lon = st.number_input("Longitude", value=-73.9855, format="%.4f")

st.markdown(f"📍 Driver H3 cell: `{h3.geo_to_h3(lat, lon, 8)}`")

run = st.button("🔍 Get Recommendations", type="primary")

if run:
    with st.spinner("Calling recommendation engine..."):
        try:
            resp = requests.get(
                f"{API_URL}/recommendation",
                params={"lat": lat, "lon": lon},
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.ConnectionError:
            st.error("API not reachable. Make sure FastAPI is running on localhost:8000")
            st.stop()
        except Exception as e:
            st.error(f"API error: {e}")
            st.stop()

    recs   = data["recommendations"]
    origin = data["driver_h3"]

    # ── Metrics row ───────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"Top {len(recs)} Recommendations")
    cols = st.columns(len(recs))
    for i, (rec, col) in enumerate(zip(recs, cols)):
        col.metric(
            label=f"#{rec['rank']} Score",
            value=f"{rec['score']:.3f}",
            delta=f"{rec['predicted_demand']:.1f} pickups"
        )

    # ── Table ─────────────────────────────────────────────────
    df_recs = pd.DataFrame(recs)[[
        "rank", "h3_cell", "predicted_demand",
        "charger_score", "grid_distance", "score"
    ]]
    df_recs.columns = ["Rank", "H3 Cell", "Pred. Demand",
                       "Charger Score", "Distance (cells)", "Final Score"]
    st.dataframe(df_recs, use_container_width=True, hide_index=True)

    # ── Build map layers ──────────────────────────────────────
    # Driver current cell — white
    origin_df = pd.DataFrame([{
        "h3": origin,
        "color": [255, 255, 255, 200],
        "label": "You are here"
    }])

    # Recommended cells — color by rank (bright → dim)
    rank_colors = [
        [255, 50,  50,  230],   # rank 1 — red
        [255, 140, 0,   210],   # rank 2 — orange
        [255, 220, 0,   200],   # rank 3 — yellow
        [100, 220, 100, 180],   # rank 4 — green
        [100, 180, 255, 160],   # rank 5 — blue
    ]
    rec_df = pd.DataFrame([
        {
            "h3":    r["h3_cell"],
            "color": rank_colors[i],
            "label": f"#{r['rank']} | Demand: {r['predicted_demand']:.1f}"
        }
        for i, r in enumerate(recs)
    ])

    driver_layer = pdk.Layer(
        "H3HexagonLayer",
        origin_df,
        get_hexagon="h3",
        get_fill_color="color",
        get_elevation=300,
        elevation_scale=1,
        extruded=True,
        pickable=True
    )

    rec_layer = pdk.Layer(
        "H3HexagonLayer",
        rec_df,
        get_hexagon="h3",
        get_fill_color="color",
        get_elevation="color[3]",
        elevation_scale=3,
        extruded=True,
        pickable=True,
        auto_highlight=True
    )

    view = pdk.ViewState(
        latitude=lat, longitude=lon,
        zoom=12, pitch=45
    )

    st.pydeck_chart(pdk.Deck(
        layers=[rec_layer, driver_layer],
        initial_view_state=view,
        tooltip={"text": "{label}"},
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
    ))

    st.markdown("""
    ⬜ **White** = Your current location  
    🔴 **Red → Blue** = Rank 1 (best) → Rank 5
    """)