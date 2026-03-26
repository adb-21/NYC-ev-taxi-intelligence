import pandas as pd

_data = {}

def load_all():
    global _data
    
    recs = pd.read_parquet("../data/recommendations", engine="pyarrow")
    recs["hour_of_day"] = recs["hour_of_day"].astype(int)
    recs["day_of_week"]  = recs["day_of_week"].astype(int)
    recs = recs.set_index(["origin_h3", "hour_of_day", "day_of_week"])
    _data["recommendations"] = recs

    demand = pd.read_parquet("../data/typical_demand_lookup", engine="pyarrow")
    demand["hour_of_day"] = demand["hour_of_day"].astype(int)
    demand["day_of_week"]  = demand["day_of_week"].astype(int)
    _data["typical_demand_lookup"] = demand

    _data["nearest_chargers"] = pd.read_parquet("../data/nearest_chargers", engine="pyarrow")

    print(f"recommendations:      {len(_data['recommendations'])} rows")
    print(f"typical_demand_lookup:{len(_data['typical_demand_lookup'])} rows")
    print(f"nearest_chargers:     {len(_data['nearest_chargers'])} rows")

def get(table: str) -> pd.DataFrame:
    return _data[table]