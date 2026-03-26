import pandas as pd
import streamlit as st

@st.cache_data
def load_demand():
    df = pd.read_parquet("../data/typical_demand_lookup", engine="pyarrow")
    df["hour_of_day"] = df["hour_of_day"].astype(int)
    df["day_of_week"]  = df["day_of_week"].astype(int)
    return df

@st.cache_data
def load_chargers():
    df = pd.read_parquet("../data/ny_chargers", engine="pyarrow")

    charger_points = df[[
        "station_name", "lat", "lon",
        "fast_chargers", "level2_chargers"
    ]].copy()

    charger_points.columns = ["name", "lat", "lon", "fast", "level2"]
    charger_points["lat"]    = pd.to_numeric(charger_points["lat"],   errors="coerce")
    charger_points["lon"]    = pd.to_numeric(charger_points["lon"],   errors="coerce")
    charger_points["fast"]   = pd.to_numeric(charger_points["fast"],  errors="coerce").fillna(0).astype(int)
    charger_points["level2"] = pd.to_numeric(charger_points["level2"],errors="coerce").fillna(0).astype(int)

    charger_points = charger_points.dropna(subset=["lat", "lon"])
    charger_points = charger_points.drop_duplicates(subset=["lat", "lon"])

    charger_points["color"] = charger_points["fast"].apply(
        lambda x: [0, 210, 100, 220] if x > 0 else [255, 200, 0, 220]
    )

    print(f"Loaded {len(charger_points)} unique charger locations")

    return charger_points

def demand_to_color(series):
    """Map demand values to RGBA — blue (low) → yellow → red (high)"""
    t = (series - series.min()) / (series.max() - series.min() + 1e-9)
    colors = []
    for val in t:
        if val < 0.5:
            r = int(255 * val * 2)
            g = int(255 * val * 2)
            b = int(255 * (1 - val * 2))
        else:
            r = 255
            g = int(255 * (1 - (val - 0.5) * 2))
            b = 0
        colors.append([r, g, b, 180])
    return colors