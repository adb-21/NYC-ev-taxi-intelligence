import h3
from data import get
import pandas as pd

def get_recommendations(lat: float, lon: float, hour: int, dow: int):
    origin_h3 = h3.geo_to_h3(lat, lon, 8)
    df = get("recommendations")

    try:
        result = df.loc[(origin_h3, hour, dow)].sort_values("rank").head(5)
    except KeyError:
        return []

    # If only one row matches, loc returns a Series — wrap it back to DataFrame
    if isinstance(result, pd.Series):
        result = result.to_frame().T

    return [
        {
            "rank":             int(r["rank"]),
            "h3_cell":          r["candidate_h3"],
            "lat":              h3.h3_to_geo(r["candidate_h3"])[0],
            "lon":              h3.h3_to_geo(r["candidate_h3"])[1],
            "predicted_demand": round(float(r["candidate_demand"]), 2),
            "charger_score":    round(float(r["charger_score"]), 2),
            "grid_distance":    int(r["grid_distance"]),
            "score":            round(float(r["final_score"]), 4)
        }
        for _, r in result.iterrows()
    ]