from fastapi import FastAPI, HTTPException
from datetime import datetime
import h3
from recommendation import get_recommendations
from data import load_all, get
from models import RecommendationResponse
from contextlib import asynccontextmanager
import pandas as pd

def safe_int(value):
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0
        return int(value)
    except (ValueError, TypeError):
        return 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_all()
    yield

app = FastAPI(
    title="NYC EV Taxi Recommendation API",
    description="Recommends high-demand pickup zones for EV taxi drivers",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
def home():
    return {"status": "ok", "message": "NYC Taxi Recommendation API"}

@app.get("/recommendation", response_model=RecommendationResponse)
def recommend(lat: float, lon: float):
    if not (40.4 <= lat <= 40.95 and -74.3 <= lon <= -73.65):
        raise HTTPException(400, "Coordinates outside NYC service area")

    now = datetime.now()
    hour, dow = now.hour, now.isoweekday()
    results = get_recommendations(lat, lon, hour, dow)

    if not results:
        raise HTTPException(404, "No recommendations found for this location/time")

    return {
        "driver_h3":       h3.geo_to_h3(lat, lon, 8),
        "hour":            hour,
        "day_of_week":     dow,
        "recommendations": results
    }

@app.get("/heatmap")
def heatmap(hour: int, dow: int):
    df = get("typical_demand_lookup")
    filtered = df[
        (df["hour_of_day"] == hour) &
        (df["day_of_week"] == dow)
    ]

    if filtered.empty:
        raise HTTPException(404, f"No heatmap data for hour={hour}, dow={dow}")

    return {
        "hour": hour,
        "day_of_week": dow,
        "cells": [
            {
                "h3":     row["pickup_h3"],
                "center": h3.h3_to_geo(row["pickup_h3"]),
                "demand": round(float(row["typical_demand"]), 2)
            }
            for _, row in filtered.iterrows()
        ]
    }

@app.get("/chargers")
def chargers(lat: float, lon: float):
    cell = h3.geo_to_h3(lat, lon, 8)
    df = get("nearest_chargers")
    filtered = df[df["h3_cell"] == cell]

    if filtered.empty:
        raise HTTPException(404, "No charger data for this location")

    r = filtered.iloc[0]
    return {
        "h3_cell": cell,
        "chargers": [
            {
                "priority": 1,
                "name":     r["P1_charger_station_name"],
                "lat":      float(r["P1_charger_lat"]),
                "lon":      float(r["P1_charger_lon"]),
                "fast":     safe_int(r["P1_charger_fast_chargers"]),
                "level2":   safe_int(r["P1_charger_level2_chargers"])
            },
            {
                "priority": 2,
                "name":     r["P2_charger_station_name"],
                "lat":      float(r["P2_charger_lat"]),
                "lon":      float(r["P2_charger_lon"]),
                "fast":     safe_int(r["P2_charger_fast_chargers"]),
                "level2":   safe_int(r["P2_charger_level2_chargers"])
            },
            {
                "priority": 3,
                "name":     r["P3_charger_station_name"],
                "lat":      float(r["P3_charger_lat"]),
                "lon":      float(r["P3_charger_lon"]),
                "fast":     safe_int(r["P3_charger_fast_chargers"]),
                "level2":   safe_int(r["P3_charger_level2_chargers"])
            }
        ]
    }